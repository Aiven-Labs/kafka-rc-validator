# Contributing

This tool is maintained by Apache Kafka PMC members. Contributions are welcome.

## Adding a New Test Phase

1. Add your test function to `validate-kafka-rc.sh` following the pattern of existing functions.
2. Use `record_result "PASS|FAIL|WARN|SKIP" "description"` to track results.
3. Add a corresponding skip flag if the test is expensive or requires specific infrastructure.
4. Call your function from `main()`.

## Updating the Kafka Version

The default Kafka version is set in `kafka-maven-validator/pom.xml` via the `kafka.version` property. The main script passes the version dynamically.

## Testing Locally

```bash
# Syntax check
bash -n validate-kafka-rc.sh

# Quick smoke test (skipping expensive phases)
SKIP_SOURCE_BUILD=true SKIP_DOCKER_TESTS=true SKIP_TIERED_STORAGE=true \
  ./validate-kafka-rc.sh <VERSION> <RC>

# Compile Java validator only
cd kafka-maven-validator && mvn compile
```

## Code Style

- Bash: 4 spaces, `set -eo pipefail`
- Java: 4 spaces, standard Java conventions
- See `.editorconfig` for details
