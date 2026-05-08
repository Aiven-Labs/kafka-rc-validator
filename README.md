# kafka-rc-validator

Automated validation tool for Apache Kafka release candidates. Designed for PMC members to efficiently verify RC artifacts before casting a vote.

Covers: PGP signatures, checksums, source build, binary distribution, Docker images, single-node quickstart, multi-broker cluster replication, Java client integration (Admin, Producer, Consumer, Streams, Connect), Kafka Connect standalone, tiered storage with S3-compatible backends, license compliance, and version consistency.

## Prerequisites

| Tool | Notes |
|------|-------|
| `curl`, `wget` | HTTP downloads |
| `gpg` | PGP signature verification |
| `sha512sum`, `sha1sum`, `md5sum` | Checksum verification |
| `java` (17+) | Source build and tests |
| `docker` | Docker image tests |
| `jq` | JSON parsing |
| `mvn` | Maven validator build |

Linux is required (`/proc/sys/kernel/random/uuid`, GNU `sed -i`).

## Quick Start

```bash
./validate-kafka-rc.sh <VERSION> <RC_NUMBER>
```

Example — validate Kafka 4.2.1 release candidate 3:

```bash
./validate-kafka-rc.sh 4.2.1 3
```

## Configuration

All configuration is via environment variables. Set them before running the script.

### Skip Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_SOURCE_BUILD` | `false` | Skip Gradle build from source |
| `SKIP_SOURCE_TESTS` | `false` | Skip unit tests after source build |
| `SKIP_DOCKER_TESTS` | `false` | Skip Docker image pull and integration tests |
| `SKIP_COMPLEX_TESTS` | `false` | Skip 3-broker cluster tests |
| `SKIP_MAVEN_TESTS` | `false` | Skip Java client validation (Maven validator) |
| `SKIP_LICENSE_CHECK` | `false` | Skip LICENSE/NOTICE compliance checks |
| `SKIP_CONNECT_TESTS` | `false` | Skip Kafka Connect standalone tests |
| `SKIP_TIERED_STORAGE` | `false` | Skip tiered storage with MinIO tests |

### Other Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `WORK_DIR` | `/tmp/kafka-rc-validation` | Workspace for downloads and test data |
| `RUN_ONLY` | (empty) | Run a single test function, e.g. `test_tiered_storage` |
| `TIERED_STORAGE_PLUGIN_VERSION` | `1.1.1` | Aiven tiered storage plugin version |
| `TIERED_STORAGE_CACHE_DIR` | `~/.cache/kafka-rc-validation/tiered-storage-plugin` | Plugin download cache |
| `MINIO_PORT` | `19000` | MinIO port for tiered storage tests |

### Examples

Skip expensive tests for a quick smoke check:

```bash
SKIP_SOURCE_BUILD=true SKIP_SOURCE_TESTS=true SKIP_DOCKER_TESTS=true \
  ./validate-kafka-rc.sh 4.2.1 3
```

Run only tiered storage validation:

```bash
RUN_ONLY=test_tiered_storage ./validate-kafka-rc.sh 4.2.1 3
```

## What Gets Validated

The script runs these phases in order:

1. **Dependency check** — verifies required tools are installed
2. **PGP key import** — downloads Apache Kafka KEYS file
3. **Artifact download** — source and binary tarballs, signatures, checksums
4. **Signature verification** — GPG verification of both archives
5. **Checksum verification** — SHA512, SHA1, and MD5
6. **URL accessibility** — release notes, Javadoc, GitHub tag, documentation
7. **Source build** — `./gradlew build -x test` (optionally with tests)
8. **Binary distribution** — validates expected scripts and JARs exist
9. **Quickstart test** — single-node KRaft broker, produce/consume
10. **Complex tests** — 3-broker KRaft cluster: replication, consumer groups, ACLs, ISR
11. **Maven validator** — Java client tests (Admin API, idempotent/transactional producers, consumers, Streams, metrics)
12. **Docker integration** — pulls RC image, creates topic, verifies broker
13. **License compliance** — LICENSE/NOTICE presence, no unexpected binaries in source
14. **Version consistency** — gradle.properties vs JAR manifest vs CLI output
15. **Kafka Connect** — standalone mode with FileStreamSource connector
16. **Tiered storage** — MinIO backend, Aiven plugin, S3 upload/read verification

## Architecture

```
kafka-rc-validator/
├── validate-kafka-rc.sh          # Bash orchestrator (all test phases)
├── kafka-maven-validator/        # Java client integration tests
│   ├── pom.xml                   # Maven build (pulls from Apache staging repo)
│   ├── run-validator.sh          # Convenience wrapper
│   └── src/main/java/.../KafkaRCValidator.java
└── README.md
```

The bash script handles infrastructure (downloading, building, starting brokers) and delegates Java client validation to the Maven sub-project. The Maven project uses the Apache staging repository to test RC artifacts before they are officially released.

## Output

On completion, the script prints a summary with PASS/FAIL/WARN/SKIP counts and a suggested vote email template ready to paste into the dev mailing list thread.

## Maven Validator (standalone)

The Java validator can also be run independently against any running Kafka cluster:

```bash
cd kafka-maven-validator
./run-validator.sh 4.2.1 localhost:9092
```

## Limitations

- Linux only (GNU sed, `/proc/sys/kernel/random/uuid`)
- KRaft mode only (no ZooKeeper)
- Docker required for several test phases
- Full run takes 30-60+ minutes depending on hardware and network

## License

Apache License 2.0. See [LICENSE](LICENSE).
