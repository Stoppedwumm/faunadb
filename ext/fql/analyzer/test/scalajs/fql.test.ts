// These tests lightly validate the fql module directly. There are also unit tests
// in scala, so this validation is mostly meant to check the API behavior of the fql
// module.

import * as fql from "../../build/scala/fql.js";

const BLANK_ENV = {
  globals: new Map(),
  shapes: new Map(),
};

describe("integration tests", () => {
  it("put integration tests here", () => {
    const ctx = fql.newQueryContext(BLANK_ENV);
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("2 + 3");
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("2 +");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual("error");
  });
});
