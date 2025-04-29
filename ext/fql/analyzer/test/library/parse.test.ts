import parse from "../../src/parse";
import * as fs from "fs";

describe("parse env", () => {
  it("can parse the static env", () => {
    const json = fs.readFileSync(
      "../../api/src/test/resources/static_environment_response_v1.json",
      "utf8"
    );
    const env = parse(JSON.parse(json));

    expect(env.globals.get("Time")).toBeDefined();
    const time = env.shapes.get("TimeModule") ?? fail("no TimeModule");
    expect(time.self).toEqual(env.globals.get("Time"));
  });
});
