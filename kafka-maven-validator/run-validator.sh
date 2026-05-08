#!/bin/bash
#
# Kafka Maven Validator Runner
# Usage: ./run-validator.sh [kafka-version] [bootstrap-servers]
#

set -e

KAFKA_VERSION="${1:-4.2.1}"
BOOTSTRAP_SERVERS="${2:-localhost:9092}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=============================================="
echo "Kafka Maven Artifact Validator"
echo "Kafka Version: $KAFKA_VERSION"
echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
echo "=============================================="
echo ""

cd "$SCRIPT_DIR"

echo "Building with Maven (using Apache Staging Repository)..."
echo "This will download Kafka ${KAFKA_VERSION} artifacts from staging..."
echo ""

mvn clean package -q -DskipTests -Dkafka.version="${KAFKA_VERSION}"

echo ""
echo "Running Kafka RC Validator..."
echo ""

java -jar target/kafka-maven-validator-1.0-SNAPSHOT.jar "$BOOTSTRAP_SERVERS"
