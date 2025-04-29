To update the `static_environment_response_v1.json` file, run this:
```
cd ext/api/src/test/resources
curl -H "Authorization: Bearer secret" http://localhost:8443/environment/1 | jq | tee static_environment_response_v1.json
```
