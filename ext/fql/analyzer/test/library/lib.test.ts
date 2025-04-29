import * as fql from "../../src/index";
import * as fs from "fs";
import parse from "../../src/parse";

const BLANK_ENV = {
  globals: new Map(),
  shapes: new Map(),
};

const json = fs.readFileSync(
  "../../api/src/test/resources/static_environment_response_v1.json",
  "utf8"
);
const STATIC_ENV = parse(JSON.parse(json));

describe("functional tests", () => {
  it("put functional tests here", () => {
    const ctx = fql.newQueryContext(BLANK_ENV);
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("2 + 3");
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("2 +");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
  });

  it("* should not explode", () => {
    const ctx = fql.newQueryContext(BLANK_ENV);

    ctx.onUpdate("2 * 3");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual("Type `2` does not have field `*`");
  });

  it("globals are not defined in blank env", () => {
    const ctx = fql.newQueryContext(BLANK_ENV);

    ctx.onUpdate("Time");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual("Unbound variable `Time`");
  });

  it("globals have a type defined in static env", () => {
    const ctx = fql.newQueryContext(STATIC_ENV);

    ctx.onUpdate("Time");
    expect(ctx.errors()).toEqual([]);

    // validate fields
    ctx.onUpdate("Time.now()");
    expect(ctx.errors()).toEqual([]);

    // validate ops
    ctx.onUpdate("2 * 3");
    expect(ctx.errors()).toEqual([]);

    // validate apply
    ctx.onUpdate("Time('2023-06-27T23:45:52.109129Z')");
    expect(ctx.errors()).toEqual([]);

    // validate access
    ctx.onUpdate("[1, 2][0]");
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("Time.foo()");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual(
      "Type `TimeModule` does not have field `foo`"
    );
  });

  it("can complete at top level", () => {
    const ctx = fql.newQueryContext(STATIC_ENV);

    const completions = ctx.completionsAt(0);
    const labels = completions.map((v) => v.label);
    expect(labels).toContain("if");
    expect(labels).toContain("at");
    expect(labels).toContain("Time");
    expect(labels).toContain("Math");

    const ifCompl = completions.find((v) => v.label == "if") ?? fail("no if");
    expect(ifCompl.replaceText).toEqual("if");
    expect(ifCompl.retrigger).toEqual(false);
    expect(ifCompl.span.start).toEqual(0);
    expect(ifCompl.span.end).toEqual(0);

    const timeCompl = completions.find((v) => v.label == "Time") ?? fail("no Time");
    expect(timeCompl.replaceText).toEqual("Time");
    expect(timeCompl.retrigger).toEqual(false);
    expect(timeCompl.span.start).toEqual(0);
    expect(timeCompl.span.end).toEqual(0);
  });

  it("can refresh", () => {
    const ctx = fql.newQueryContext(STATIC_ENV);

    ctx.onUpdate("Time");
    expect(ctx.errors()).toEqual([]);

    // refresh should re-parse and update errors.
    ctx.refresh(BLANK_ENV);

    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual("Unbound variable `Time`");
  });
});
