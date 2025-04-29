import * as fql from "fql-analyzer";

export interface AnalyzerConfig {
  endpoint?: string,
  secret?: string
}

export class AnalyzerLoader {
  private config: Required<AnalyzerConfig> = {
    endpoint: "https://db.fauna.com",
    secret: ""
  };

  private analyzer: fql.FQLAnalyzer | undefined;

  async getAnalyzer(config?: AnalyzerConfig): Promise<fql.FQLAnalyzer> {
    const {endpoint, secret} = config ?? {};
    if (secret === "") {
      throw new Error("Missing database secret");
    }

    let configChanged = false;
    if (endpoint != undefined && endpoint != "" && endpoint != this.config.endpoint) {
      this.config.endpoint = endpoint;
      configChanged = true;
    }
    if (secret != undefined && secret != this.config.secret) {
      this.config.secret = secret;
      configChanged = true;
    }
    if (configChanged || this.analyzer == undefined) {
      this.analyzer = await fql.loadAnalyzer({
        endpoint: new URL(this.config.endpoint),
        secret: this.config.secret,
      });
    }

    return this.analyzer;
  }
}
