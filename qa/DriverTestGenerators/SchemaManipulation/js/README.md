# Prereqs

- Node 20+
- Fauna database secret for the tests

# Running the tests

1. Set env vars

```
export FAUNA_ENDPOINT=https://db.fauna.com
export FAUNA_SECRET=fnA...
export FAUNA_QAHOST=123.123.123.123 # For QA use; optional
```

2. Execute `init.sh` to install npm modules (if missing) and install `fauna-shell`

```
./init.sh
```

3. Execute `run.sh` to run `node index.js` that runs the test logic

```
./run.sh
```

4. [Optional] If running in QA, pause queries to `FAUNA_QAHOST`

```
touch $FAUNA_QAHOST.pause

# Delete file to unpause
rm -f $FAUNA_QAHOST.pause
```
