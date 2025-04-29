import * as fql from "../build/scala/fql.js";
import fetch from "cross-fetch";
import parse from "./parse";

export type Environment = {
  globals: Map<string, TypeScheme>;
  shapes: Map<string, TypeShape>;
};

export type TypeShape = {
  self: TypeScheme;
  fields: Map<string, TypeScheme>;
  ops: Map<string, TypeScheme>;
  apply?: TypeScheme;
  access?: TypeScheme;
  alias?: TypeScheme;
};

export type TypeScheme = typeof fql.TypeScheme;

type Opts = {
  endpoint: URL;
  secret: string;
};

/**
 * Parses the environment from core, and returns a blank context.
 */
export async function loadAnalyzer(options: {
  // Database URL. defaults to https://db.fauna.com. This should not include `/environment/1`
  endpoint?: URL;
  // A database secret
  secret: string;
}): Promise<FQLAnalyzer> {
  const opts = {
    endpoint: new URL("htps://db.fauna.com"),
    ...options,
  };

  return await FQLAnalyzer.newAnalyzer(opts);
}

export class FQLAnalyzer {
  opts: Opts;
  environment: Environment;

  contexts: QueryContext[] = [];

  static async newAnalyzer(opts: Opts): Promise<FQLAnalyzer> {
    const analyzer = new FQLAnalyzer(opts);
    await analyzer.reload();
    return analyzer;
  }

  private constructor(opts: Opts) {
    this.opts = opts;
    // NOTE: This is set in reload(), which is always called in the newAnalyzer function.
    this.environment = null as any;
  }

  /**
   * Reloads the query context for the given schema version.
   * throws an EnvironmentError when loading the environment fails.
   */
  async reload(_schemaVersion?: number): Promise<void> {
    let res;
    try {
      res = await fetch(new URL("/environment/1", this.opts.endpoint), {
        headers: { authorization: `Bearer ${this.opts.secret}` },
      });
    } catch (e) {
      console.error(e);
      throw new EnvironmentError(
        "unavailable",
        "Failed to reach the endpoint to obtain the query environment.",
        e
      );
    }

    const respJson = await res.json();
    if (res.status == 200) {
      this.environment = parse(await respJson);
      for (const ctx of this.contexts) {
        ctx.refresh(this.environment);
      }
    } else {
      throw new EnvironmentError(respJson.code, respJson.message);
    }
  }

  /**
   * Gets a new QueryContext, configured for an FQL file. This should be called
   * once for each new tab opened.
   *
   * Deprecated in favor of getQueryContext.
   */
  getContext(): QueryContext {
    return this.getQueryContext();
  }

  /**
   * Gets a new QueryContext, configured for an FQL file. This should be called
   * once for each new tab opened.
   */
  getQueryContext(): QueryContext {
    const ctx = newQueryContext(this.environment);
    this.contexts.push(ctx);
    return ctx;
  }

  /**
   * Gets a new QueryContext, configured for an FSL item. This should be called
   * once for each schema item opened.
   */
  getSchemaItemContext(
    kind: SchemaItemKind | undefined,
    name: string | undefined
  ): QueryContext {
    const ctx = newSchemaItemContext(this.environment, kind, name);
    this.contexts.push(ctx);
    return ctx;
  }

  /**
   * Gets a new QueryContext, configured for an FSL file. This should be called
   * once for each schema file opened.
   */
  getSchemaFileContext(path: string | undefined): QueryContext {
    const ctx = newSchemaFileContext(this.environment, path);
    this.contexts.push(ctx);
    return ctx;
  }
}

/**
 * Parses the environment from core, and returns a blank context. This should be
 * used when the client cannot authenticate, and it needs to load a static
 * environment from a JSON file.
 */
export function newQueryContext(env: Environment): QueryContext {
  return fql.newQueryContext(env);
}

/**
 * Parses the environment from core, and returns a blank context. This should be
 * used when the client cannot authenticate, and it needs to load a static
 * environment from a JSON file.
 */
export function newSchemaItemContext(
  env: Environment,
  kind: SchemaItemKind | undefined,
  name: string | undefined
): QueryContext {
  return fql.newSchemaItemContext(env, kind, name);
}

/**
 * Parses the environment from core, and returns a blank context. This should be
 * used when the client cannot authenticate, and it needs to load a static
 * environment from a JSON file.
 */
export function newSchemaFileContext(
  env: Environment,
  path: string | undefined
): QueryContext {
  return fql.newSchemaFileContext(env, path);
}

/**
 * A long-lived context, which should be reused as much as possible. Any time
 * the user enters a character, this context should be updated with the new
 * query.
 */
export interface QueryContext {
  /**
   * Refreshes this context with the given environment.
   */
  refresh(env: Environment): void;

  /**
   * Updates the context with the given query. This will re-parse the query if
   * needed.
   *
   * This should be called whenever the query text changes (so any time a key is
   * pressed).
   */
  onUpdate(query: string): void;

  /**
   * Returns all errors from the last onUpdate() call.
   */
  errors(): Array<Diagnostic>;

  /**
   * Returns the completions at the given cursor.
   *
   * This should be called any time the client wants to display completions.
   */
  completionsAt(cursor: Position): Array<CompletionItem>;

  /**
   * Returns the type of the value under the cursor.
   *
   * This should be called any time a user hovers their mouse over some text.
   */
  hoverOn(cursor: Position): string | undefined;

  /**
   * Returns a position to goto for the item under the cursor.
   *
   * This should be called upon pressing the goto definition keybind.
   */
  gotoDefinitionAt(cursor: Position): Position | undefined;

  /**
   * Returns a list of spans to highlight. This is used whenever the user hovers
   * their cursor over an identifier, and it will highlight all identifiers with
   * the same value.
   *
   * This should be called any time the cursor is moved, and left stationary for
   * a short amount of time.
   */
  highlightAt(cursor: Position): Array<HighlightSpan>;
}

/**
 * The position of a character in the query.
 */
export type Position = number;

/**
 * A range of characters in the query.
 */
export interface Span {
  /**
   * The first character, inclusive.
   */
  start: Position;
  /**
   * The last character, exclusive.
   */
  end: Position;
  /**
   * The source file of this span.
   */
  src: Src;
}

/**
 * A reference to the user's source code.
 */
export interface Src {
  /**
   * The name of the source file. If the name is `*query*`, then this is the
   * user's query.
   */
  name: string;
}

/**
 * A diagnostic, or a warning or error from typechecking/parsing.
 */
export interface Diagnostic {
  /**
   * The span to underline for this diagnostic.
   */
  span: Span;

  /**
   * The severity of this diagnostic (error or warning).
   */
  severity: Severity;

  /**
   * The message of this diagnostic. This will contain a user-readable error message.
   */
  message: string;

  /**
   * Any related information. These should be underlined the same way a Diagnostic is underlined.
   */
  relatedInfo: Array<DiagnosticInfo>;
}

/**
 * Related information to a diagnostic, such as a hint.
 */
export interface DiagnosticInfo {
  /**
   * The span of source code to underline.
   */
  span: Span;

  /**
   * The message for this related information.
   */
  message: string;
}

/**
 * A diagnostic's severity.
 */
export enum Severity {
  Error = "error",
  Warn = "warn",
  Hint = "hint",
  Cause = "cause",
}

/**
 * A possible completion item. If selected, the text 'replaceText' should be
 * inserted at 'span'.
 */
export interface CompletionItem {
  /**
   * The text to display to the user for this completion item.
   */
  label: string;

  /**
   * Contains docs of the given function in markdown.
   */
  detail: string;

  /**
   * Span to insert the 'replaceText' at.
   */
  span: Span;
  /**
   * Text to insert when this completion item is selected.
   */
  replaceText: string;

  /**
   * New cursor position after placing replaceText in the query.
   */
  newCursor: number;

  /**
   * The kind of completion item. This changes which icon is used when
   * displaying this item.
   */
  kind: CompletionItemKind;

  /**
   * If true, completions should re-appear after selecting this item.
   */
  retrigger: boolean;

  /**
   * If set, this snippet should be inserted at `span` instead of `replaceText`.
   */
  snippet: string | undefined;
}

/**
 * A kind of completion. This changes the icon shown next to the completion
 * item.
 */
export enum CompletionItemKind {
  Module = "module",
  Keyword = "keyword",
  Type = "type",
  Variable = "variable",
  Field = "field",
  Property = "property",
  Function = "function",
}

/**
 * A span to highlight.
 */
export interface HighlightSpan {
  span: Span;
  kind: HighlightKind;
}

/**
 * This will be set depending if the variable was defined or simply used
 * at the given span. This should be used to change the highlight color.
 */
export enum HighlightKind {
  Read = "read",
  Write = "write",
}

/**
 * This error is thrown when the analyzer has an
 * error loading the query environment.
 */
export class EnvironmentError extends Error {
  code: string;
  cause?: any;
  constructor(code: string, message: string, cause?: any) {
    super(message);
    this.code = code;
    this.cause = cause;
  }
}

/**
 * A kind of schema item, for example a function or collection.
 */
export enum SchemaItemKind {
  Function = "function",
  Collection = "collection",
  Role = "role",
  AccessProvider = "access_provider",
}
