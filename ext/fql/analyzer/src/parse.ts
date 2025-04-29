import * as fql from "../build/scala/fql.js";
import { Environment, TypeScheme } from "./index";

class ParseError extends Error {}

/**
 * Parses a JSON blob to an Environment.
 */
export default function parse(json: any): Environment {
  const env = json.environment;

  return {
    globals: parseTypeSchemeMap(env.globals),
    shapes: new Map(
      parseMap(env.types).map(([k, v]) => [
        k,
        {
          self: parseTypeScheme(v.self),
          fields: v.fields == undefined ? new Map() : parseTypeSchemeMap(v?.fields),
          ops: v.ops == undefined ? new Map() : parseTypeSchemeMap(v?.ops),
          apply: v.apply == undefined ? undefined : parseTypeScheme(v.apply),
          access: v.access == undefined ? undefined : parseTypeScheme(v.access),
          alias: v.alias == undefined ? undefined : parseTypeScheme(v.alias),
        },
      ])
    ),
  };
}

function parseString(v: any): string {
  if (typeof v === "string") {
    return v;
  } else {
    throw new ParseError(`value ${v} is not a string`);
  }
}
function parseTypeScheme(v: any): TypeScheme | undefined {
  return fql.typeSchemeFromString(parseString(v));
}
function parseMap(v: any): [string, any][] {
  if (typeof v === "object") {
    return Object.entries(v);
  } else {
    throw new ParseError(`value ${v} is not a map`);
  }
}

function parseTypeSchemeMap(v: any): Map<string, TypeScheme> {
  return new Map(
    parseMap(v).flatMap(([k, v]) => {
      const ts = parseTypeScheme(v);
      return ts == undefined ? [] : [[k, ts]];
    })
  );
}
