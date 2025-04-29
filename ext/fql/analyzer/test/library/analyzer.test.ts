import * as fql from "../../src/index";
import { execSync } from "child_process";

function sbt(command: string) {
  try {
    const output = execSync(`cd ../../../; sbt ${command}; cd -`);
    console.log(`started core: ${output}`);
  } catch (error) {
    console.error(`Failed to start core: ${error}`);
    throw error;
  }
}

describe("functinoal tests", () => {
  beforeAll(() => sbt("runCore"));

  it("can request the environment from core", async () => {
    const analyzer = await fql.loadAnalyzer({
      endpoint: new URL("http://127.0.0.1:8443"),
      secret: "secret",
    });

    const globals = analyzer.environment.globals;
    const shapes = analyzer.environment.shapes;
    expect(globals.get("Time")).toBeDefined();
    const time = shapes.get("TimeModule") ?? fail("no module TimeModule");
    expect(time.self).toEqual(globals.get("Time"));

    const ctx = analyzer.getQueryContext();

    ctx.onUpdate("Time.now()");
    expect(ctx.errors()).toEqual([]);

    ctx.onUpdate("Time.foo()");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual(
      "Type `TimeModule` does not have field `foo`"
    );
  });

  it("can reload the analyzer", async () => {
    const analyzer = await fql.loadAnalyzer({
      endpoint: new URL("http://127.0.0.1:8443"),
      secret: "secret",
    });

    const ctx = analyzer.getQueryContext();

    ctx.onUpdate("User");
    expect(ctx.errors().length).toEqual(1);
    expect(ctx.errors()[0].severity).toEqual(fql.Severity.Error);
    expect(ctx.errors()[0].message).toEqual("Unbound variable `User`");

    // drivers are for nerds
    await fetch(new URL("http://127.0.0.1:8443/query/1"), {
      method: "POST",
      body: JSON.stringify({ query: "Collection.create({ name: 'User' })" }),
      headers: { authorization: "Bearer secret" },
    });

    await analyzer.reload();

    // after the reload, User should be defined.
    expect(ctx.errors()).toEqual([]);
  });
});
