# QA Test Generators

Child directories in this path each define a QA Test Generator that can be used in the QA cluster test runs in the database Concourse pipeline. Each directory needs to match a class definition in `src/main/scala/qa/DriverTestGenerators.scala` and will need to be added to QA runs in `ops/concourse/pipelines/core/pipeline.yml`.

## Requirements

- Each directory name must match the class definition in the core code (case-sensitive)
- Each directory must have at least one of the following driver language subdirectories: `[js, python, go, java, dotnet]`
- Each language subdirectory must have the following:
  - An app that consumes the Fauna driver for that language and runs queries against `FAUNA_ENDPOINT` using secret `FAUNA_SECRET`
  - `init.sh`, that does any setup needed for the app (e.g. `npm install`)
    - NB: File must be executable - `chmod +x` before committing
  - `run.sh`, which calls the app in the language runtime (e.g. `node index.js`)

## App Functionality

The app in each language should support the following functionality:
- Consume environment variables `FAUNA_ENDPOINT` (e.g. `http://localhost:8443`) and `FAUNA_SECRET` to configure the queries
- Consume environment variable `FAUNA_QAHOST` and monitor the working directory for a file named `$FAUNA_QAHOST.pause` - if this file exists, pause queries until it is deleted
  - See `StreamingWithDriver/js/run-queries.js` L17 for how that looks in JS
- Should log to stdout and stderr using whatever log module is preferable, but the log lines should ideally include: UTC timestamp, log level, message, and stack trace if it's an error
- Optionally can handle a `SIGTERM` signal from the caller to gracefully shutdown
