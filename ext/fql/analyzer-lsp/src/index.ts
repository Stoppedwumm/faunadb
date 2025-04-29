import * as fql from "fql-analyzer";

import {
  createConnection,
  TextDocuments,
  Diagnostic,
  DiagnosticSeverity,
  ProposedFeatures,
  InitializeParams,
  DidChangeConfigurationNotification,
  CompletionItem,
  CompletionItemKind,
  Hover,
  HoverParams,
  TextDocumentPositionParams,
  TextDocumentSyncKind,
  InitializeResult,
  LocationLink,
  DocumentHighlight,
  DocumentHighlightKind,
  InlayHintParams,
  InlayHint,
  InsertTextFormat,
} from "vscode-languageserver/node.js";

import { TextDocument } from "vscode-languageserver-textdocument";
import { AnalyzerConfig, AnalyzerLoader } from "./AnalyzerLoader";

/** This is here so an unhandled exception doesn't cause the server to go down. */
process.on("uncaughtException", (error) => {
  console.error("Unhandled exception:", error);
});

const analyzerLoader = new AnalyzerLoader();
const openFiles = new Map<string, [TextDocument, fql.QueryContext]>();

const connection = createConnection(ProposedFeatures.all);

// Manages open documents, and handles diffs from the client so that we can just work with plain documents.
const documents: TextDocuments<TextDocument> = new TextDocuments(TextDocument);

let hasConfigurationCapability = false;
let hasWorkspaceFolderCapability = false;
let hasDiagnosticRelatedInformationCapability = false;

connection.onInitialize((params: InitializeParams) => {
  const capabilities = params.capabilities;

  // Does the client support the `workspace/configuration` request?
  // If not, we fall back using global settings.
  hasConfigurationCapability = !!(
    capabilities.workspace && !!capabilities.workspace.configuration
  );
  hasWorkspaceFolderCapability = !!(
    capabilities.workspace && !!capabilities.workspace.workspaceFolders
  );
  hasDiagnosticRelatedInformationCapability = !!(
    capabilities.textDocument &&
    capabilities.textDocument.publishDiagnostics &&
    capabilities.textDocument.publishDiagnostics.relatedInformation
  );

  const result: InitializeResult = {
    capabilities: {
      textDocumentSync: TextDocumentSyncKind.Incremental,
      // Tell the client that this server supports code completion.
      completionProvider: {
        // Trigger completions after pressing any of these characters.
        triggerCharacters: [".", "("],
        // If we want to resolve each function when the user scrolls over it,
        // enable this. This is probably more performant, but it's easier to
        // just send everything on the initial completion request.
        resolveProvider: false,
      },
      // TODO: Implement
      /*
      definitionProvider: true,
      documentHighlightProvider: true,
      inlayHintProvider: true,
      hoverProvider: true,
      */
    },
  };
  if (hasWorkspaceFolderCapability) {
    result.capabilities.workspace = {
      workspaceFolders: {
        supported: true,
      },
    };
  }
  return result;
});

type ConfigParams = {
  secret: string;
  endpoint?: string;
};

connection.onRequest("fauna/setConfig", async (params: ConfigParams) => {
  const secret = params.secret;
  if (typeof secret !== "string" || secret.trim() == "") {
    return {
      status: "error",
      code: "unauthorized",
      message: "Invalid secret provided, must be a non empty string.",
    };
  }

  const config: AnalyzerConfig = {
    secret: secret,
  };

  const endpoint = params.endpoint;
  if (typeof endpoint === "string" && endpoint.trim() != "") {
    config.endpoint = endpoint;
  }

  try {
    await analyzerLoader.getAnalyzer(config);
    return {
      status: "success",
    };
  } catch (e) {
    if (e instanceof fql.EnvironmentError) {
      return {
        status: "error",
        code: e.code,
        message: e.message,
      };
    } else {
      return {
        status: "error",
        code: "internal_failure",
        message: "Unexpected failure loading query environment.",
      };
    }
  }
});

type RefreshParams = {
  schema_version: number;
};

connection.onRequest("fauna/refresh", async (params: RefreshParams) => {
  const version = params.schema_version;
  if (typeof version !== "number") {
    return {
      status: "error",
      code: "invalid",
      message: "Invalid schema version provided, must be a number.",
    };
  }

  try {
    const analyzer = await analyzerLoader.getAnalyzer();
    await analyzer.reload(version);
    return {
      status: "success",
    };
  } catch (e) {
    if (e instanceof fql.EnvironmentError) {
      return {
        status: "error",
        code: e.code,
        message: e.message,
      };
    } else {
      return {
        status: "error",
        code: "internal_failure",
        message: "Unexpected failure loading query environment.",
      };
    }
  }
});

connection.onInitialized(() => {
  if (hasConfigurationCapability) {
    // Register for all configuration changes.
    connection.client.register(
      DidChangeConfigurationNotification.type,
      undefined,
    );
  }
  if (hasWorkspaceFolderCapability) {
    connection.workspace.onDidChangeWorkspaceFolders((_event) => {
      connection.console.log("Workspace folder change event received.");
    });
  }
});

// The example settings
interface ExampleSettings {
  maxNumberOfProblems: number;
}

// The global settings, used when the `workspace/configuration` request is not supported by the client.
// Please note that this is not the case when using this server with the client provided in this example
// but could happen with other clients.
// const defaultSettings: ExampleSettings = { maxNumberOfProblems: 1000 };
// let globalSettings: ExampleSettings = defaultSettings;

// Cache the settings of all open documents
const documentSettings: Map<string, Thenable<ExampleSettings>> = new Map();

connection.onDidChangeConfiguration((_change) => {
  if (hasConfigurationCapability) {
    // Reset all cached document settings
    documentSettings.clear();
  } else {
    /*
    globalSettings = <ExampleSettings>(
      (change.settings.languageServerExample || defaultSettings)
    );
    */
  }

  // Revalidate all open text documents
  documents.all().forEach(validateTextDocument);
});

// Only keep settings for open documents
documents.onDidClose((e) => {
  documentSettings.delete(e.document.uri);
});

// The content of a text document has changed. This event is emitted
// when the text document first opened or when its content has changed.
documents.onDidChangeContent((change) => {
  validateTextDocument(change.document);
});

function getContextForDoc(
  analyzer: fql.FQLAnalyzer,
  doc: TextDocument,
): fql.QueryContext {
  if (doc.uri.endsWith(".fsl")) {
    // TODO: What should this path be? Its unused in core right now, but we
    // probably want it to be a path relative to the project root or something.
    return analyzer.getSchemaFileContext(doc.uri.toString());
  } else {
    // TODO: What to do with non-.fql files? For now we'll just assume they're
    // FQL.
    return analyzer.getQueryContext();
  }
}

async function validateTextDocument(doc: TextDocument): Promise<void> {
  const analyzer = await analyzerLoader.getAnalyzer();

  const res = openFiles.get(doc.uri);
  const context = res == undefined ? getContextForDoc(analyzer, doc) : res[1];
  context.onUpdate(doc.getText());
  openFiles.set(doc.uri, [doc, context]);

  const diagnostics = context.errors().map((error) => {
    const diagnostic: Diagnostic = {
      severity: DiagnosticSeverity.Error,
      range: {
        start: doc.positionAt(error.span.start),
        end: doc.positionAt(error.span.end),
      },
      message: error.message,
      source: "fql-analyzer",
    };
    if (hasDiagnosticRelatedInformationCapability) {
      diagnostic.relatedInformation = error.relatedInfo.map((info) => ({
        location: {
          uri: doc.uri,
          range: {
            start: doc.positionAt(error.span.start),
            end: doc.positionAt(error.span.end),
          },
        },
        message: info.message,
      }));
    }
    return diagnostic;
  });

  connection.sendDiagnostics({ uri: doc.uri, diagnostics });
}

connection.onDidChangeWatchedFiles((_change) => {
  // TODO: Re-create analyzer based on new settings.
  connection.console.log("We received an file change event");
});

// This handler provides the initial list of the completion items.
connection.onCompletion(
  (docPos: TextDocumentPositionParams): CompletionItem[] => {
    const docContext = openFiles.get(docPos.textDocument.uri);
    if (docContext == undefined) {
      return [];
    }
    const [doc, context] = docContext;

    const pos = doc.offsetAt(docPos.position);
    const completions = context.completionsAt(pos);

    const items = [];
    for (const compl of completions) {
      const kind: CompletionItemKind = {
        [fql.CompletionItemKind.Module]: CompletionItemKind.Module,
        [fql.CompletionItemKind.Keyword]: CompletionItemKind.Keyword,
        [fql.CompletionItemKind.Type]: CompletionItemKind.Class,
        [fql.CompletionItemKind.Variable]: CompletionItemKind.Variable,
        [fql.CompletionItemKind.Field]: CompletionItemKind.Field,
        [fql.CompletionItemKind.Function]: CompletionItemKind.Function,
        [fql.CompletionItemKind.Property]: CompletionItemKind.Property,
      }[compl.kind];

      const item: CompletionItem = {
        label: compl.label,
        labelDetails: {
          description: compl.detail,
        },
        detail: compl.detail,
        kind: kind,
        ...(compl.retrigger && {
          command: {
            command: "editor.action.triggerSuggest",
            title: "Re-trigger completions...",
          },
        }),
        // This is used as a sorting key when comparing against other items.
        // Because we want to maintain ordering, just use the length of the
        // array as the key, and pad it with zeros to make sure it orders
        // correctly.
        sortText: items.length.toString().padStart(5, "0"),
        insertText: compl.snippet ?? compl.replaceText,
        insertTextFormat: InsertTextFormat.Snippet,
      };

      items.push(item);
    }
    return items;
  },
);

connection.onHover((docPos: HoverParams): Hover | undefined => {
  const docContext = openFiles.get(docPos.textDocument.uri);
  if (docContext == undefined) {
    return undefined;
  }
  const [doc, context] = docContext;

  const pos = doc.offsetAt(docPos.position);
  const hover = context.hoverOn(pos);

  if (hover == undefined) {
    return undefined;
  } else {
    return {
      contents: `\`${hover}\``,
    };
  }
});

connection.onDefinition(
  (docPos: TextDocumentPositionParams): LocationLink[] => {
    const docContext = openFiles.get(docPos.textDocument.uri);
    if (docContext == undefined) {
      return [];
    }
    const [doc, context] = docContext;

    const pos = doc.offsetAt(docPos.position);
    const result = context.gotoDefinitionAt(pos);

    if (result != undefined) {
      return [
        {
          targetUri: docPos.textDocument.uri,
          targetRange: {
            start: doc.positionAt(result),
            end: doc.positionAt(result),
          },
          targetSelectionRange: {
            start: doc.positionAt(result),
            end: doc.positionAt(result),
          },
        },
      ];
    } else {
      return [];
    }
  },
);

connection.onDocumentHighlight(
  (docPos: TextDocumentPositionParams): DocumentHighlight[] => {
    const docContext = openFiles.get(docPos.textDocument.uri);
    if (docContext == undefined) {
      return [];
    }
    const [doc, context] = docContext;

    const pos = doc.offsetAt(docPos.position);
    const results = context.highlightAt(pos);

    const out = [];
    for (const result of results) {
      let kind: DocumentHighlightKind = DocumentHighlightKind.Text;
      kind = DocumentHighlightKind.Read;
      // if (result.kind == "read") {
      //   kind = DocumentHighlightKind.Read;
      // } else if (result.kind == "write") {
      //   kind = DocumentHighlightKind.Write;
      // }
      out.push({
        range: {
          start: doc.positionAt(result.span.start),
          end: doc.positionAt(result.span.end),
        },
        kind: kind,
      });
    }
    return out;
  },
);

connection.onRequest(
  "textDocument/inlayHint",
  (params: InlayHintParams): InlayHint[] => {
    const docContext = openFiles.get(params.textDocument.uri);
    if (docContext == undefined) {
      return [];
    }
    const [doc, _context] = docContext;

    // const hints = context.inlayHints();
    const hints: any[] = [];

    const out = [];
    for (const hint of hints) {
      out.push({
        position: doc.positionAt(hint.position),
        label: ": " + hint.text,
      });
    }
    return out;
  },
);

// This handler resolves additional information for the item selected in
// the completion list.
//
// TODO: It might be more performant to provide docs here, but providing them
// in `onCompletion` is much simpler.
/*
connection.onCompletionResolve((item: CompletionItem): CompletionItem => {
  return item;
});
*/

documents.listen(connection);

connection.listen();
