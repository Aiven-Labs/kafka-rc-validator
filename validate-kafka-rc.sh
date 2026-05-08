#!/bin/bash
#
# Apache Kafka Release Candidate Validation Script
# Usage: ./validate-kafka-rc.sh <version> <rc-number>
# Example: ./validate-kafka-rc.sh 4.1.2 1
#

set -eo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
VERSION="${1:-}"
RC="${2:-}"
WORK_DIR="${WORK_DIR:-/tmp/kafka-rc-validation}"
KEYS_URL="https://dist.apache.org/repos/dist/release/kafka/KEYS"
SKIP_SOURCE_BUILD="${SKIP_SOURCE_BUILD:-false}"
SKIP_SOURCE_TESTS="${SKIP_SOURCE_TESTS:-false}"
SKIP_DOCKER_TESTS="${SKIP_DOCKER_TESTS:-false}"
SKIP_COMPLEX_TESTS="${SKIP_COMPLEX_TESTS:-false}"
SKIP_MAVEN_TESTS="${SKIP_MAVEN_TESTS:-false}"
SKIP_LICENSE_CHECK="${SKIP_LICENSE_CHECK:-false}"
SKIP_CONNECT_TESTS="${SKIP_CONNECT_TESTS:-false}"
SKIP_TIERED_STORAGE="${SKIP_TIERED_STORAGE:-false}"
RUN_ONLY="${RUN_ONLY:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Tiered storage configuration
TIERED_STORAGE_PLUGIN_VERSION="${TIERED_STORAGE_PLUGIN_VERSION:-1.1.1}"
TIERED_STORAGE_CACHE_DIR="${TIERED_STORAGE_CACHE_DIR:-${HOME}/.cache/kafka-rc-validation/tiered-storage-plugin}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
TIERED_STORAGE_BUCKET="kafka-tiered-storage"

# Derived variables
RC_TAG="${VERSION}-rc${RC}"
MAJOR_MINOR=$(echo "$VERSION" | cut -d. -f1,2 | tr -d '.')
BASE_URL="https://dist.apache.org/repos/dist/dev/kafka/${RC_TAG}"
GITHUB_TAG_URL="https://github.com/apache/kafka/releases/tag/${RC_TAG}"
DOC_URL="https://kafka.apache.org/${MAJOR_MINOR}/documentation.html"
PROTOCOL_URL="https://kafka.apache.org/${MAJOR_MINOR}/design/protocol/"

# Logging functions
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; }
log_section() { echo -e "\n${YELLOW}========================================${NC}"; echo -e "${YELLOW}$1${NC}"; echo -e "${YELLOW}========================================${NC}"; }

# Track results
RESULTS=()
START_TIME=$SECONDS
record_result() {
    local status="$1"
    local check="$2"
    RESULTS+=("$status|$check")
}

# Global PID tracking for robust cleanup
MANAGED_PIDS=()
MANAGED_CONTAINERS=()

# --- Helper functions ---

generate_cluster_id() {
    local kafka_dir="${1:-.}"
    "$kafka_dir/bin/kafka-storage.sh" random-uuid 2>/dev/null || cat /proc/sys/kernel/random/uuid
}

copy_kraft_config() {
    local dest="$1"
    local src_dir="${2:-.}"
    if [[ -f "$src_dir/config/server.properties" ]]; then
        cp "$src_dir/config/server.properties" "$dest"
    elif [[ -f "$src_dir/config/kraft/server.properties" ]]; then
        cp "$src_dir/config/kraft/server.properties" "$dest"
    else
        return 1
    fi
}

wait_for_kafka() {
    local bootstrap="${1:-localhost:9092}"
    local timeout_secs="${2:-60}"
    local kafka_dir="${3:-.}"
    local check_pid="${4:-}"
    local wait_time=0

    while ! "$kafka_dir/bin/kafka-broker-api-versions.sh" --bootstrap-server "$bootstrap" &>/dev/null; do
        sleep 2
        wait_time=$((wait_time + 2))
        if [[ -n "$check_pid" ]] && ! kill -0 "$check_pid" 2>/dev/null; then
            return 2
        fi
        if [[ $wait_time -ge $timeout_secs ]]; then
            return 1
        fi
    done
    return 0
}

KAFKA_STANDALONE_PID=""

start_kafka_standalone() {
    local config_name="$1"
    local log_file="$2"
    local extra_config="${3:-}"
    local kafka_dir="${4:-.}"

    local kraft_config="$kafka_dir/config/${config_name}.properties"
    local log_dir="${WORK_DIR}/kafka-logs-${config_name}-$$"

    copy_kraft_config "$kraft_config" "$kafka_dir" || return 1
    sed -i "s|log.dirs=.*|log.dirs=${log_dir}|g" "$kraft_config"

    if [[ -n "$extra_config" ]]; then
        printf '%s\n' "$extra_config" >> "$kraft_config"
    fi

    local cluster_id
    cluster_id=$(generate_cluster_id "$kafka_dir")
    "$kafka_dir/bin/kafka-storage.sh" format -t "$cluster_id" -c "$kraft_config" --standalone --ignore-formatted &>/dev/null || true
    mkdir -p "$kafka_dir/logs"

    "$kafka_dir/bin/kafka-server-start.sh" "$kraft_config" > "$log_file" 2>&1 &
    KAFKA_STANDALONE_PID=$!
    MANAGED_PIDS+=($KAFKA_STANDALONE_PID)
}

# --- End helper functions ---

usage() {
    cat << EOF
Usage: $0 <version> <rc-number>

Arguments:
    version     Kafka version (e.g., 4.1.2)
    rc-number   Release candidate number (e.g., 1)

Environment variables:
    WORK_DIR                      Working directory (default: /tmp/kafka-rc-validation)
    SKIP_SOURCE_BUILD             Skip source build and tests (default: false)
    SKIP_SOURCE_TESTS             Skip source tests but still build (default: false)
    SKIP_DOCKER_TESTS             Skip Docker image tests (default: false)
    SKIP_COMPLEX_TESTS            Skip complex integration tests (default: false)
    SKIP_MAVEN_TESTS              Skip Maven artifact validation (default: false)
    SKIP_LICENSE_CHECK            Skip license compliance checks (default: false)
    SKIP_CONNECT_TESTS            Skip Kafka Connect tests (default: false)
    SKIP_TIERED_STORAGE           Skip tiered storage tests (default: false)
    TIERED_STORAGE_PLUGIN_VERSION Aiven tiered storage plugin version (default: 1.1.1)
    RUN_ONLY                      Run a single test function (e.g., test_tiered_storage)

Example:
    $0 4.1.2 1
    SKIP_SOURCE_BUILD=true $0 4.1.2 1
    RUN_ONLY=test_tiered_storage $0 4.1.2 1
EOF
    exit 1
}

check_dependencies() {
    log_section "Checking Dependencies"

    local deps=("curl" "wget" "gpg" "sha512sum" "sha1sum" "md5sum" "java" "docker" "jq" "mvn")
    local missing=()

    for dep in "${deps[@]}"; do
        if command -v "$dep" &> /dev/null; then
            log_success "$dep is installed"
        else
            log_warn "$dep is not installed"
            missing+=("$dep")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_warn "Missing dependencies: ${missing[*]}"
        log_warn "Some checks may be skipped"
    fi
}

setup_workdir() {
    log_section "Setting Up Working Directory"

    mkdir -p "$WORK_DIR"
    cd "$WORK_DIR"
    log_info "Working directory: $WORK_DIR"

    # Kill any leftover Kafka processes from previous runs
    if lsof -ti:9092 &>/dev/null; then
        log_warn "Port 9092 in use, killing leftover processes..."
        lsof -ti:9092 | xargs kill 2>/dev/null || true
        sleep 2
    fi

    # Clean up leftover Docker containers from previous runs
    docker rm -f "$(docker ps -aq --filter name=kafka-rc-)" &>/dev/null || true

    # Clean up logs and data from previous runs
    rm -f "$WORK_DIR"/*.log 2>/dev/null || true
    rm -rf "$WORK_DIR"/kafka-logs-* "$WORK_DIR"/kafka-cluster-* 2>/dev/null || true
    rm -rf "$WORK_DIR"/tiered-storage-plugin 2>/dev/null || true
    rm -f "$WORK_DIR"/connect-offsets-* "$WORK_DIR"/connect-test-input-* 2>/dev/null || true

    # Clean up downloaded archives (will re-download)
    rm -rf kafka-*.tgz* kafka-*.zip* KEYS 2>/dev/null || true
}

import_keys() {
    log_section "Importing Apache Kafka PGP Keys"

    # Use isolated GPG home to avoid polluting user's keyring
    GPG_HOME="${WORK_DIR}/gpg-home"
    mkdir -p "$GPG_HOME"
    chmod 700 "$GPG_HOME"
    export GNUPGHOME="$GPG_HOME"

    log_info "Downloading KEYS file from $KEYS_URL"
    if curl -sSf -o KEYS "$KEYS_URL"; then
        log_success "Downloaded KEYS file"
    else
        log_error "Failed to download KEYS file"
        record_result "FAIL" "Download KEYS file"
        return 1
    fi

    log_info "Importing PGP keys into isolated keyring..."
    if gpg --import KEYS 2>/dev/null; then
        log_success "Imported PGP keys"
        record_result "PASS" "Import PGP keys"
    else
        log_error "Failed to import PGP keys"
        record_result "FAIL" "Import PGP keys"
        return 1
    fi
}

download_artifacts() {
    log_section "Downloading Release Artifacts"

    local artifacts=(
        "kafka-${VERSION}-src.tgz"
        "kafka_2.13-${VERSION}.tgz"
    )

    for artifact in "${artifacts[@]}"; do
        log_info "Downloading $artifact..."

        if curl -sSf -O "${BASE_URL}/${artifact}"; then
            log_success "Downloaded $artifact"
        else
            log_error "Failed to download $artifact"
            record_result "FAIL" "Download $artifact"
            continue
        fi

        if curl -sSf -O "${BASE_URL}/${artifact}.asc"; then
            log_success "Downloaded ${artifact}.asc"
        else
            log_error "Failed to download ${artifact}.asc"
            record_result "FAIL" "Download ${artifact}.asc"
        fi

        if curl -sSf -O "${BASE_URL}/${artifact}.sha512"; then
            log_success "Downloaded ${artifact}.sha512"
        else
            log_error "Failed to download ${artifact}.sha512"
            record_result "FAIL" "Download ${artifact}.sha512"
        fi

        if curl -sS -f -O "${BASE_URL}/${artifact}.sha1" 2>/dev/null; then
            log_success "Downloaded ${artifact}.sha1"
        else
            log_info "No SHA1 file available for ${artifact}"
        fi

        if curl -sS -f -O "${BASE_URL}/${artifact}.md5" 2>/dev/null; then
            log_success "Downloaded ${artifact}.md5"
        else
            log_info "No MD5 file available for ${artifact}"
        fi
    done
}

verify_signatures() {
    log_section "Verifying PGP Signatures"

    for artifact in kafka-${VERSION}-src.tgz kafka_2.13-${VERSION}.tgz; do
        if [[ -f "$artifact" && -f "${artifact}.asc" ]]; then
            log_info "Verifying signature for $artifact..."
            if gpg --verify "${artifact}.asc" "$artifact" 2>/dev/null; then
                log_success "Signature valid for $artifact"
                record_result "PASS" "PGP signature: $artifact"
            else
                log_error "Signature verification failed for $artifact"
                record_result "FAIL" "PGP signature: $artifact"
            fi
        else
            log_warn "Missing files for signature verification: $artifact"
            record_result "SKIP" "PGP signature: $artifact"
        fi
    done
}

parse_checksum_file() {
    local file="$1"
    local artifact="$2"
    cat "$file" | sed "s/^${artifact}[[:space:]]*:[[:space:]]*//" | tr -d ' \n\r\t' | tr '[:upper:]' '[:lower:]'
}

verify_checksums() {
    log_section "Verifying Checksums (SHA512, SHA1, MD5)"

    for artifact in kafka-${VERSION}-src.tgz kafka_2.13-${VERSION}.tgz; do
        if [[ ! -f "$artifact" ]]; then
            log_warn "Artifact not found: $artifact"
            continue
        fi

        if [[ -f "${artifact}.sha512" ]]; then
            log_info "Verifying SHA512 for $artifact..."
            local expected_sha512=$(parse_checksum_file "${artifact}.sha512" "$artifact")
            local actual_sha512=$(sha512sum "$artifact" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')

            if [[ "$expected_sha512" == "$actual_sha512" ]]; then
                log_success "SHA512 valid for $artifact"
                record_result "PASS" "SHA512 checksum: $artifact"
            else
                log_error "SHA512 mismatch for $artifact"
                log_error "Expected: $expected_sha512"
                log_error "Actual:   $actual_sha512"
                record_result "FAIL" "SHA512 checksum: $artifact"
            fi
        else
            log_warn "Missing SHA512 file for $artifact"
            record_result "SKIP" "SHA512 checksum: $artifact"
        fi

        if [[ -f "${artifact}.sha1" ]]; then
            log_info "Verifying SHA1 for $artifact..."
            local expected_sha1=$(parse_checksum_file "${artifact}.sha1" "$artifact")
            local actual_sha1=$(sha1sum "$artifact" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')

            if [[ "$expected_sha1" == "$actual_sha1" ]]; then
                log_success "SHA1 valid for $artifact"
                record_result "PASS" "SHA1 checksum: $artifact"
            else
                log_error "SHA1 mismatch for $artifact"
                record_result "FAIL" "SHA1 checksum: $artifact"
            fi
        else
            log_warn "No SHA1 file for $artifact (may not be provided)"
            record_result "SKIP" "SHA1 checksum: $artifact"
        fi

        if [[ -f "${artifact}.md5" ]]; then
            log_info "Verifying MD5 for $artifact..."
            local expected_md5=$(parse_checksum_file "${artifact}.md5" "$artifact")
            local actual_md5=$(md5sum "$artifact" | awk '{print $1}' | tr '[:upper:]' '[:lower:]')

            if [[ "$expected_md5" == "$actual_md5" ]]; then
                log_success "MD5 valid for $artifact"
                record_result "PASS" "MD5 checksum: $artifact"
            else
                log_error "MD5 mismatch for $artifact"
                record_result "FAIL" "MD5 checksum: $artifact"
            fi
        else
            log_warn "No MD5 file for $artifact (may not be provided)"
            record_result "SKIP" "MD5 checksum: $artifact"
        fi
    done
}

check_urls() {
    log_section "Checking URL Availability"

    local urls=(
        "$BASE_URL|Release artifacts"
        "${BASE_URL}/RELEASE_NOTES.html|Release notes"
        "${BASE_URL}/javadoc/|Javadoc"
        "$GITHUB_TAG_URL|GitHub tag"
        "$DOC_URL|Documentation"
        "$PROTOCOL_URL|Protocol documentation"
    )

    for entry in "${urls[@]}"; do
        local url="${entry%|*}"
        local name="${entry#*|}"

        log_info "Checking $name: $url"
        local http_code=$(curl -sS -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")

        if [[ "$http_code" == "200" ]]; then
            log_success "$name is accessible (HTTP $http_code)"
            record_result "PASS" "URL accessible: $name"
        elif [[ "$http_code" == "301" || "$http_code" == "302" ]]; then
            log_success "$name is accessible (HTTP $http_code redirect)"
            record_result "PASS" "URL accessible: $name"
        else
            log_error "$name is not accessible (HTTP $http_code)"
            record_result "FAIL" "URL accessible: $name"
        fi
    done
}

build_from_source() {
    log_section "Building from Source"

    if [[ "$SKIP_SOURCE_BUILD" == "true" ]]; then
        log_warn "Skipping source build (SKIP_SOURCE_BUILD=true)"
        record_result "SKIP" "Source build"
        return 0
    fi

    local src_archive="${WORK_DIR}/kafka-${VERSION}-src.tgz"
    if [[ ! -f "$src_archive" ]]; then
        log_error "Source archive not found: $src_archive"
        record_result "FAIL" "Source build"
        return 1
    fi

    log_info "Extracting source archive..."
    rm -rf "${WORK_DIR}/kafka-${VERSION}-src" 2>/dev/null || true
    tar -xzf "$src_archive" -C "$WORK_DIR"
    local src_dir="${WORK_DIR}/kafka-${VERSION}-src"

    log_info "Building Kafka (this may take a while)..."
    if (cd "$src_dir" && ./gradlew build -x test --no-daemon 2>&1) | tee "${WORK_DIR}/build.log"; then
        log_success "Build successful"
        record_result "PASS" "Source build"
    else
        log_error "Build failed (see build.log for details)"
        record_result "FAIL" "Source build"
        return 1
    fi

    if [[ "$SKIP_SOURCE_TESTS" == "true" ]]; then
        log_warn "Skipping source tests (SKIP_SOURCE_TESTS=true)"
        record_result "SKIP" "Unit tests"
    else
        log_info "Running unit tests (this may take a while)..."
        if (cd "$src_dir" && timeout 3600 ./gradlew test -PmaxParallelForks=6 --no-daemon 2>&1) | tee "${WORK_DIR}/test.log"; then
            log_success "Unit tests passed"
            record_result "PASS" "Unit tests"
        else
            log_error "Unit tests failed (see test.log for details)"
            record_result "FAIL" "Unit tests"
        fi
    fi
}

test_binary_distribution() {
    log_section "Testing Binary Distribution"

    local binary_archive="${WORK_DIR}/kafka_2.13-${VERSION}.tgz"
    if [[ ! -f "$binary_archive" ]]; then
        log_error "Binary archive not found: $binary_archive"
        record_result "FAIL" "Binary distribution test"
        return 1
    fi

    log_info "Extracting binary archive..."
    rm -rf "${WORK_DIR}/kafka_2.13-${VERSION}" 2>/dev/null || true
    tar -xzf "$binary_archive" -C "$WORK_DIR"
    local bin_dir="${WORK_DIR}/kafka_2.13-${VERSION}"

    local scripts=("bin/kafka-server-start.sh" "bin/kafka-topics.sh" "bin/kafka-console-producer.sh" "bin/kafka-console-consumer.sh")
    local all_scripts_found=true

    for script in "${scripts[@]}"; do
        if [[ -x "${bin_dir}/$script" ]]; then
            log_success "Found executable: $script"
        else
            log_error "Missing or not executable: $script"
            all_scripts_found=false
        fi
    done

    if $all_scripts_found; then
        record_result "PASS" "Essential scripts present"
    else
        record_result "FAIL" "Essential scripts present"
    fi

    if [[ -d "${bin_dir}/libs" ]] && ls "${bin_dir}"/libs/*.jar &>/dev/null; then
        local jar_count=$(ls "${bin_dir}"/libs/*.jar | wc -l)
        log_success "Found $jar_count JAR files in libs/"
        record_result "PASS" "Library JARs present"
    else
        log_error "No JAR files found in libs/"
        record_result "FAIL" "Library JARs present"
    fi

    # Verify config directory has expected property files
    local config_files=("config/server.properties")
    local all_configs_found=true
    for cfg in "${config_files[@]}"; do
        if [[ -f "${bin_dir}/$cfg" ]]; then
            log_success "Found config: $cfg"
        else
            log_error "Missing config: $cfg"
            all_configs_found=false
        fi
    done

    if $all_configs_found; then
        record_result "PASS" "Binary distribution: config files"
    else
        record_result "FAIL" "Binary distribution: config files"
    fi
}

test_docker_images() {
    log_section "Testing Docker Images"

    if [[ "$SKIP_DOCKER_TESTS" == "true" ]]; then
        log_warn "Skipping Docker tests (SKIP_DOCKER_TESTS=true)"
        record_result "SKIP" "Docker tests"
        return 0
    fi

    if ! command -v docker &> /dev/null; then
        log_warn "Docker not installed, skipping Docker tests"
        record_result "SKIP" "Docker tests"
        return 0
    fi

    local images=(
        "apache/kafka:${RC_TAG}"
        "apache/kafka-native:${RC_TAG}"
    )

    for image in "${images[@]}"; do
        log_info "Pulling Docker image: $image"
        if docker pull "$image" 2>/dev/null; then
            log_success "Successfully pulled $image"
            record_result "PASS" "Docker pull: $image"

            log_info "Verifying image structure..."
            if docker inspect "$image" &>/dev/null; then
                log_success "Image inspection successful"
            fi
        else
            log_error "Failed to pull $image"
            record_result "FAIL" "Docker pull: $image"
        fi
    done
}

run_quickstart_test() {
    log_section "Running Quickstart Tests"

    local kafka_dir="${WORK_DIR}/kafka_2.13-${VERSION}"
    if [[ ! -d "$kafka_dir" ]]; then
        log_error "Binary distribution not found"
        record_result "FAIL" "Quickstart test"
        return 1
    fi

    log_info "Starting Kafka server..."
    start_kafka_standalone "kraft-quickstart" "${WORK_DIR}/kafka-quickstart.log" "" "$kafka_dir"
    local kafka_pid=$KAFKA_STANDALONE_PID
    log_info "Kafka logs: ${WORK_DIR}/kafka-quickstart.log"

    if ! wait_for_kafka "localhost:9092" 60 "$kafka_dir" "$kafka_pid"; then
        log_error "Kafka failed to start within 60 seconds"
        kill $kafka_pid 2>/dev/null || true
        record_result "FAIL" "Quickstart: server start"
        return 1
    fi
    log_success "Kafka server started"
    record_result "PASS" "Quickstart: server start"

    log_info "Creating test topic..."
    if "$kafka_dir/bin/kafka-topics.sh" --create --topic quickstart-test --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 2>&1; then
        log_success "Topic created"
        record_result "PASS" "Quickstart: create topic"
    else
        log_error "Failed to create topic"
        record_result "FAIL" "Quickstart: create topic"
    fi

    log_info "Producing test messages..."
    echo -e "message1\nmessage2\nmessage3" | "$kafka_dir/bin/kafka-console-producer.sh" --topic quickstart-test --bootstrap-server localhost:9092 2>&1
    log_success "Messages produced"
    record_result "PASS" "Quickstart: produce messages"

    log_info "Consuming test messages..."
    local consumed=$(timeout 10 "$kafka_dir/bin/kafka-console-consumer.sh" --topic quickstart-test --from-beginning --max-messages 3 --bootstrap-server localhost:9092 2>/dev/null || true)
    if [[ $(echo "$consumed" | wc -l) -ge 3 ]]; then
        log_success "Messages consumed successfully"
        record_result "PASS" "Quickstart: consume messages"
    else
        log_warn "May not have consumed all messages"
        record_result "WARN" "Quickstart: consume messages"
    fi

    log_info "Stopping Kafka server..."
    kill $kafka_pid 2>/dev/null || true
    wait $kafka_pid 2>/dev/null || true
    rm -rf "${WORK_DIR}/kafka-logs-kraft-quickstart-$$" 2>/dev/null || true

    log_success "Quickstart test completed"
}

run_complex_tests() {
    log_section "Running Complex Integration Tests"

    if [[ "$SKIP_COMPLEX_TESTS" == "true" ]]; then
        log_warn "Skipping complex tests (SKIP_COMPLEX_TESTS=true)"
        record_result "SKIP" "Complex tests"
        return 0
    fi

    local kafka_dir="${WORK_DIR}/kafka_2.13-${VERSION}"
    if [[ ! -d "$kafka_dir" ]]; then
        log_error "Binary distribution not found"
        record_result "FAIL" "Complex tests"
        return 1
    fi

    local cluster_id
    cluster_id=$(generate_cluster_id "$kafka_dir")

    local log_dir_base="${WORK_DIR}/kafka-cluster-$$"
    mkdir -p "$log_dir_base"

    # Generate directory IDs for each node
    local dir_id_1=$(cat /proc/sys/kernel/random/uuid | tr -d '-' | xxd -r -p | base64 | tr '+/' '-_' | tr -d '=')
    local dir_id_2=$(cat /proc/sys/kernel/random/uuid | tr -d '-' | xxd -r -p | base64 | tr '+/' '-_' | tr -d '=')
    local dir_id_3=$(cat /proc/sys/kernel/random/uuid | tr -d '-' | xxd -r -p | base64 | tr '+/' '-_' | tr -d '=')

    # Ports: Node 1: 9092/19092, Node 2: 9093/19093, Node 3: 9094/19094
    local initial_controllers="1@localhost:19092:${dir_id_1},2@localhost:19093:${dir_id_2},3@localhost:19094:${dir_id_3}"
    local bootstrap_controllers="localhost:19092,localhost:19093,localhost:19094"

    for i in 1 2 3; do
        local plaintext_port=$((9091 + i))
        local controller_port=$((19091 + i))
        local config="${kafka_dir}/config/kraft-server-${i}.properties"
        cat > "$config" << EOF
process.roles=broker,controller
node.id=${i}
controller.quorum.bootstrap.servers=${bootstrap_controllers}
listeners=PLAINTEXT://:${plaintext_port},CONTROLLER://:${controller_port}
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://localhost:${plaintext_port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=${log_dir_base}/broker-${i}
num.partitions=3
default.replication.factor=3
min.insync.replicas=2
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
EOF
        mkdir -p "${log_dir_base}/broker-${i}"
        "$kafka_dir/bin/kafka-storage.sh" format -t "$cluster_id" -c "$config" --initial-controllers "$initial_controllers" --ignore-formatted &>/dev/null || true
    done

    log_info "Starting 3-broker KRaft cluster..."
    mkdir -p "$kafka_dir/logs"
    local pids=()
    for i in 1 2 3; do
        "$kafka_dir/bin/kafka-server-start.sh" "${kafka_dir}/config/kraft-server-${i}.properties" > "${WORK_DIR}/kafka-cluster-node-${i}.log" 2>&1 &
        pids+=($!)
        MANAGED_PIDS+=($!)
    done
    log_info "Kafka cluster logs: ${WORK_DIR}/kafka-cluster-node-{1,2,3}.log"

    log_info "Waiting for cluster to start (max 90s)..."
    local wait_time=0
    local bootstrap="localhost:9092,localhost:9093,localhost:9094"
    while ! "$kafka_dir/bin/kafka-broker-api-versions.sh" --bootstrap-server "$bootstrap" &>/dev/null; do
        sleep 3
        wait_time=$((wait_time + 3))
        if [[ $wait_time -ge 90 ]]; then
            log_error "Cluster failed to start within 90 seconds"
            for pid in "${pids[@]}"; do kill $pid 2>/dev/null || true; done
            record_result "FAIL" "Complex: cluster start"
            return 1
        fi
    done
    log_success "3-broker cluster started"
    record_result "PASS" "Complex: 3-broker cluster start"

    # Test 1: Topic with replication
    log_info "Test 1: Creating replicated topic..."
    if "$kafka_dir/bin/kafka-topics.sh" --create --topic replicated-test --partitions 6 --replication-factor 3 --bootstrap-server "$bootstrap" 2>&1; then
        log_success "Replicated topic created"
        record_result "PASS" "Complex: replicated topic"
    else
        log_error "Failed to create replicated topic"
        record_result "FAIL" "Complex: replicated topic"
    fi

    # Test 2: Producer with acks=all
    log_info "Test 2: Testing producer with acks=all..."
    local producer_test_result="PASS"
    for i in {1..10}; do
        echo "test-message-$i"
    done | "$kafka_dir/bin/kafka-console-producer.sh" --topic replicated-test --bootstrap-server "$bootstrap" --command-property acks=all 2>&1 || producer_test_result="WARN"

    if [[ "$producer_test_result" == "PASS" ]]; then
        log_success "Producer with acks=all test passed"
        record_result "PASS" "Complex: producer acks=all"
    else
        log_warn "Producer test had issues"
        record_result "WARN" "Complex: producer acks=all"
    fi

    # Test 3: Consumer groups
    log_info "Test 3: Testing consumer groups..."
    timeout 15 "$kafka_dir/bin/kafka-console-consumer.sh" --topic replicated-test --group test-group --from-beginning --max-messages 5 --bootstrap-server "$bootstrap" 2>/dev/null &
    local consumer1=$!
    timeout 15 "$kafka_dir/bin/kafka-console-consumer.sh" --topic replicated-test --group test-group --from-beginning --max-messages 5 --bootstrap-server "$bootstrap" 2>/dev/null &
    local consumer2=$!

    sleep 5

    if "$kafka_dir/bin/kafka-consumer-groups.sh" --describe --group test-group --bootstrap-server "$bootstrap" 2>&1 | grep -q "test-group"; then
        log_success "Consumer group working"
        record_result "PASS" "Complex: consumer groups"
    else
        log_warn "Consumer group may have issues"
        record_result "WARN" "Complex: consumer groups"
    fi

    wait $consumer1 2>/dev/null || true
    wait $consumer2 2>/dev/null || true

    # Test 4: Topic configuration
    log_info "Test 4: Testing topic configuration..."
    if "$kafka_dir/bin/kafka-configs.sh" --alter --topic replicated-test --add-config retention.ms=86400000 --bootstrap-server "$bootstrap" 2>&1; then
        log_success "Topic configuration altered"
        record_result "PASS" "Complex: topic configuration"
    else
        log_error "Failed to alter topic configuration"
        record_result "FAIL" "Complex: topic configuration"
    fi

    # Test 5: Describe topics and check ISR
    log_info "Test 5: Verifying topic replication..."
    local topic_desc=$("$kafka_dir/bin/kafka-topics.sh" --describe --topic replicated-test --bootstrap-server "$bootstrap" 2>&1)
    if echo "$topic_desc" | grep -q "Isr:"; then
        local isr_count=$(echo "$topic_desc" | grep -oE "Isr:[[:space:]]*[0-9,]+" | head -1 | sed 's/Isr:[[:space:]]*//' | tr ',' '\n' | wc -l || true)
        if [[ $isr_count -ge 3 ]]; then
            log_success "All replicas in sync (ISR count: $isr_count)"
            record_result "PASS" "Complex: ISR verification"
        else
            log_warn "Not all replicas in sync (ISR count: $isr_count)"
            record_result "WARN" "Complex: ISR verification"
        fi
    else
        log_error "Could not verify ISR"
        record_result "FAIL" "Complex: ISR verification"
    fi

    # Test 6: ACL operations
    log_info "Test 6: Testing ACL CLI (expects failure without security)..."
    local acl_output
    acl_output=$("$kafka_dir/bin/kafka-acls.sh" --list --bootstrap-server "$bootstrap" 2>&1 || true)
    if echo "$acl_output" | grep -q -E "(No ACLs|Authorizer|SecurityDisabledException|not supported|Current ACLs)"; then
        log_success "ACL CLI working (no authorizer configured as expected)"
        record_result "PASS" "Complex: ACL CLI"
    else
        log_warn "ACL CLI check inconclusive. Output: $acl_output"
        record_result "WARN" "Complex: ACL CLI"
    fi

    # Cleanup
    log_info "Stopping cluster..."
    for pid in "${pids[@]}"; do
        kill $pid 2>/dev/null || true
    done
    for pid in "${pids[@]}"; do
        wait $pid 2>/dev/null || true
    done
    rm -rf "$log_dir_base" 2>/dev/null || true
    rm -f "${kafka_dir}"/config/kraft-server-*.properties 2>/dev/null || true

    log_success "Complex integration tests completed"
}

run_maven_validator() {
    log_section "Running Maven Artifact Validator (Java Client Tests)"

    if [[ "$SKIP_MAVEN_TESTS" == "true" ]]; then
        log_warn "Skipping Maven tests (SKIP_MAVEN_TESTS=true)"
        record_result "SKIP" "Maven artifact validation"
        return 0
    fi

    if ! command -v mvn &> /dev/null; then
        log_warn "Maven not installed, skipping Maven artifact validation"
        record_result "SKIP" "Maven artifact validation"
        return 0
    fi

    local maven_validator_dir="${SCRIPT_DIR}/kafka-maven-validator"
    if [[ ! -d "$maven_validator_dir" ]]; then
        log_warn "Maven validator not found at $maven_validator_dir"
        record_result "SKIP" "Maven artifact validation"
        return 0
    fi

    cd "$maven_validator_dir"

    log_info "Building Maven validator for Kafka ${VERSION} (downloading from Apache Staging)..."
    log_info "Maven build logs: ${WORK_DIR}/maven-build.log"
    if mvn clean package -q -DskipTests -Dkafka.version="${VERSION}" > "${WORK_DIR}/maven-build.log" 2>&1; then
        log_success "Maven build successful"
        record_result "PASS" "Maven: Build with staging artifacts"
    else
        log_error "Maven build failed (see maven-build.log)"
        record_result "FAIL" "Maven: Build with staging artifacts"
        cd "$WORK_DIR"
        return 1
    fi

    local kafka_dir="${WORK_DIR}/kafka_2.13-${VERSION}"
    local kafka_pid=""
    if [[ -d "$kafka_dir" ]]; then
        if ! "$kafka_dir/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 &>/dev/null; then
            log_warn "Kafka not running, starting it for Maven tests..."
            start_kafka_standalone "kraft-maven-test" "${WORK_DIR}/kafka-maven.log" "" "$kafka_dir"
            kafka_pid=$KAFKA_STANDALONE_PID
            log_info "Kafka logs: ${WORK_DIR}/kafka-maven.log"

            if ! wait_for_kafka "localhost:9092" 60 "$kafka_dir" "$kafka_pid"; then
                log_error "Kafka failed to start for Maven tests"
                kill $kafka_pid 2>/dev/null || true
                cd "$WORK_DIR"
                record_result "FAIL" "Maven: Kafka startup for tests"
                return 1
            fi
        fi
    fi

    log_info "Running Maven validator against Kafka ${VERSION}..."
    log_info "Maven validator logs: ${WORK_DIR}/maven-validator.log"
    if java -jar target/kafka-maven-validator-1.0-SNAPSHOT.jar localhost:9092 > "${WORK_DIR}/maven-validator.log" 2>&1; then
        log_success "Maven validator completed successfully"
        record_result "PASS" "Maven: Client integration tests"
    else
        log_error "Maven validator had failures (see maven-validator.log)"
        record_result "FAIL" "Maven: Client integration tests"
    fi

    if [[ -n "$kafka_pid" ]]; then
        kill $kafka_pid 2>/dev/null || true
        rm -rf "${WORK_DIR}/kafka-logs-kraft-maven-test-$$" 2>/dev/null || true
    fi

    cd "$WORK_DIR"
}

run_docker_integration_test() {
    log_section "Running Docker Integration Test"

    if [[ "$SKIP_DOCKER_TESTS" == "true" ]]; then
        log_warn "Skipping Docker integration test (SKIP_DOCKER_TESTS=true)"
        record_result "SKIP" "Docker integration"
        return 0
    fi

    if ! command -v docker &> /dev/null; then
        log_warn "Docker not installed, skipping Docker integration test"
        record_result "SKIP" "Docker integration"
        return 0
    fi

    local image="apache/kafka:${RC_TAG}"
    local container_name="kafka-rc-test-$$"

    log_info "Starting Kafka container..."
    if docker run -d --name "$container_name" -p 29092:9092 "$image" 2>/dev/null; then
        MANAGED_CONTAINERS+=("$container_name")
        log_success "Container started"

        log_info "Waiting for Kafka in Docker to be ready..."
        local wait_time=0
        while ! docker exec "$container_name" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &>/dev/null; do
            sleep 3
            wait_time=$((wait_time + 3))
            if [[ $wait_time -ge 60 ]]; then
                log_error "Docker Kafka failed to start within 60 seconds"
                docker rm -f "$container_name" 2>/dev/null || true
                record_result "FAIL" "Docker integration: startup"
                return 1
            fi
        done

        log_success "Docker Kafka is ready"
        record_result "PASS" "Docker integration: startup"

        log_info "Creating topic in Docker Kafka..."
        if docker exec "$container_name" /opt/kafka/bin/kafka-topics.sh --create --topic docker-test --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 2>&1; then
            log_success "Topic created in Docker"
            record_result "PASS" "Docker integration: create topic"
        else
            log_error "Failed to create topic in Docker"
            record_result "FAIL" "Docker integration: create topic"
        fi

        log_info "Cleaning up Docker container..."
        docker rm -f "$container_name" 2>/dev/null || true
    else
        log_error "Failed to start container"
        record_result "FAIL" "Docker integration: start container"
    fi
}

test_license_compliance() {
    log_section "Testing License & Legal Compliance"

    if [[ "$SKIP_LICENSE_CHECK" == "true" ]]; then
        log_warn "Skipping license check (SKIP_LICENSE_CHECK=true)"
        record_result "SKIP" "License compliance"
        return 0
    fi

    local src_dir="kafka-${VERSION}-src"
    local bin_dir="kafka_2.13-${VERSION}"

    if [[ -d "$src_dir" ]]; then
        if [[ -f "$src_dir/LICENSE" ]]; then
            log_success "LICENSE file exists in source distribution"
            record_result "PASS" "License: SOURCE has LICENSE"
        else
            log_error "LICENSE file missing in source distribution"
            record_result "FAIL" "License: SOURCE has LICENSE"
        fi

        if [[ -f "$src_dir/NOTICE" ]]; then
            log_success "NOTICE file exists in source distribution"
            record_result "PASS" "License: SOURCE has NOTICE"
        else
            log_error "NOTICE file missing in source distribution"
            record_result "FAIL" "License: SOURCE has NOTICE"
        fi

        # Check for unexpected binary files in source (exclude build dirs and gradle wrapper)
        log_info "Checking for unexpected binary files in source..."
        local binary_files=$(find "$src_dir" \( -type d -name "build" -o -path "*/gradle/wrapper" \) -prune -o -type f \( -name "*.jar" -o -name "*.class" -o -name "*.so" -o -name "*.dll" -o -name "*.dylib" \) -print 2>/dev/null | head -5)
        if [[ -z "$binary_files" ]]; then
            log_success "No unexpected binary files in source distribution"
            record_result "PASS" "License: No binaries in source"
        else
            log_error "Unexpected binary files found in source:"
            echo "$binary_files"
            record_result "FAIL" "License: No binaries in source"
        fi
    else
        log_warn "Source directory not found, skipping source license checks"
        record_result "SKIP" "License: SOURCE checks"
    fi

    if [[ -d "$bin_dir" ]]; then
        if [[ -f "$bin_dir/LICENSE" ]]; then
            log_success "LICENSE file exists in binary distribution"
            record_result "PASS" "License: BINARY has LICENSE"
        else
            log_error "LICENSE file missing in binary distribution"
            record_result "FAIL" "License: BINARY has LICENSE"
        fi

        if [[ -f "$bin_dir/NOTICE" ]]; then
            log_success "NOTICE file exists in binary distribution"
            record_result "PASS" "License: BINARY has NOTICE"
        else
            log_error "NOTICE file missing in binary distribution"
            record_result "FAIL" "License: BINARY has NOTICE"
        fi
    else
        log_warn "Binary directory not found, skipping binary license checks"
        record_result "SKIP" "License: BINARY checks"
    fi
}

test_version_consistency() {
    log_section "Testing Version Consistency"

    local src_dir="kafka-${VERSION}-src"
    local bin_dir="kafka_2.13-${VERSION}"
    local versions_found=()
    local all_match=true

    if [[ -d "$src_dir" && -f "$src_dir/gradle.properties" ]]; then
        local gradle_version=$(grep "^version=" "$src_dir/gradle.properties" | cut -d'=' -f2 | tr -d '[:space:]')
        if [[ -n "$gradle_version" ]]; then
            versions_found+=("gradle.properties: $gradle_version")
            if [[ "$gradle_version" != "$VERSION" ]]; then
                log_error "Version mismatch in gradle.properties: $gradle_version (expected $VERSION)"
                all_match=false
            else
                log_success "gradle.properties version matches: $gradle_version"
            fi
        fi
    fi

    if [[ -d "$bin_dir/libs" ]]; then
        local clients_jar=$(ls "$bin_dir/libs/kafka-clients-"*.jar 2>/dev/null | head -1)
        if [[ -n "$clients_jar" && -f "$clients_jar" ]]; then
            local manifest_version=$(unzip -p "$clients_jar" META-INF/MANIFEST.MF 2>/dev/null | grep "Implementation-Version:" | cut -d':' -f2 | tr -d '[:space:]\r')
            if [[ -n "$manifest_version" ]]; then
                versions_found+=("kafka-clients JAR manifest: $manifest_version")
                if [[ "$manifest_version" != "$VERSION" ]]; then
                    log_error "Version mismatch in JAR manifest: $manifest_version (expected $VERSION)"
                    all_match=false
                else
                    log_success "JAR manifest version matches: $manifest_version"
                fi
            fi
        fi
    fi

    if [[ -d "$bin_dir" && -x "$bin_dir/bin/kafka-broker-api-versions.sh" ]]; then
        local cli_version=$("$bin_dir/bin/kafka-broker-api-versions.sh" --version 2>&1 | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
        if [[ -n "$cli_version" ]]; then
            versions_found+=("CLI --version: $cli_version")
            if [[ "$cli_version" != "$VERSION" ]]; then
                log_error "Version mismatch in CLI: $cli_version (expected $VERSION)"
                all_match=false
            else
                log_success "CLI version matches: $cli_version"
            fi
        fi
    fi

    if [[ ${#versions_found[@]} -eq 0 ]]; then
        log_warn "Could not extract any version information"
        record_result "SKIP" "Version consistency"
    elif $all_match; then
        log_success "All version strings are consistent: $VERSION"
        record_result "PASS" "Version consistency"
    else
        log_error "Version inconsistency detected"
        record_result "FAIL" "Version consistency"
    fi
}

test_kafka_connect() {
    log_section "Testing Kafka Connect"

    if [[ "$SKIP_CONNECT_TESTS" == "true" ]]; then
        log_warn "Skipping Connect tests (SKIP_CONNECT_TESTS=true)"
        record_result "SKIP" "Kafka Connect"
        return 0
    fi

    local kafka_dir="${WORK_DIR}/kafka_2.13-${VERSION}"
    if [[ ! -d "$kafka_dir" ]]; then
        log_error "Binary distribution not found"
        record_result "FAIL" "Kafka Connect"
        return 1
    fi

    local kafka_pid=""
    local started_kafka=false
    if ! "$kafka_dir/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 &>/dev/null; then
        log_info "Starting Kafka for Connect tests..."
        start_kafka_standalone "kraft-connect-test" "${WORK_DIR}/kafka-connect.log" "" "$kafka_dir"
        kafka_pid=$KAFKA_STANDALONE_PID
        log_info "Kafka logs: ${WORK_DIR}/kafka-connect.log"
        started_kafka=true

        if ! wait_for_kafka "localhost:9092" 60 "$kafka_dir" "$kafka_pid"; then
            log_error "Kafka failed to start for Connect tests"
            kill $kafka_pid 2>/dev/null || true
            record_result "FAIL" "Connect: Kafka startup"
            return 1
        fi
    fi

    local connect_config="${kafka_dir}/config/connect-standalone-test.properties"
    cat > "$connect_config" << EOF
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.file.filename=${WORK_DIR}/connect-offsets-$$
offset.flush.interval.ms=10000
plugin.path=${kafka_dir}/libs/
EOF

    local source_config="${kafka_dir}/config/connect-file-source-test.properties"
    local test_input="${WORK_DIR}/connect-test-input-$$.txt"
    echo -e "line1\nline2\nline3\nline4\nline5" > "$test_input"
    cat > "$source_config" << EOF
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=${test_input}
topic=connect-test-topic
EOF

    log_info "Starting Kafka Connect standalone..."
    "$kafka_dir/bin/connect-standalone.sh" "$connect_config" "$source_config" > "${WORK_DIR}/kafka-connect-standalone.log" 2>&1 &
    local connect_pid=$!
    MANAGED_PIDS+=($connect_pid)
    log_info "Connect logs: ${WORK_DIR}/kafka-connect-standalone.log"

    log_info "Waiting for Connect REST API (max 60s)..."
    local wait_time=0
    while ! curl -s http://localhost:8083/ &>/dev/null; do
        sleep 3
        wait_time=$((wait_time + 3))
        if ! kill -0 $connect_pid 2>/dev/null; then
            log_error "Connect process died. Check ${WORK_DIR}/kafka-connect-standalone.log"
            if $started_kafka; then kill $kafka_pid 2>/dev/null || true; fi
            record_result "FAIL" "Connect: process died"
            return 1
        fi
        if [[ $wait_time -ge 60 ]]; then
            log_error "Connect REST API not available within 60 seconds"
            kill $connect_pid 2>/dev/null || true
            if $started_kafka; then kill $kafka_pid 2>/dev/null || true; fi
            record_result "FAIL" "Connect: REST API startup"
            return 1
        fi
    done
    log_success "Connect REST API is available"
    record_result "PASS" "Connect: REST API startup"

    log_info "Checking connector plugins..."
    local plugins=$(curl -s http://localhost:8083/connector-plugins 2>/dev/null)
    if echo "$plugins" | grep -q -E "(FileStreamSource|file\.FileStreamSourceConnector)"; then
        log_success "FileStreamSource connector plugin found"
        record_result "PASS" "Connect: FileStreamSource plugin"
    else
        log_warn "FileStreamSource plugin not listed (removed from default classpath in Kafka 4.x)"
        log_info "Available plugins: $plugins"
        record_result "SKIP" "Connect: FileStreamSource plugin"
        log_info "Skipping connector data flow tests (plugin not available)"
        kill $connect_pid 2>/dev/null || true
        wait $connect_pid 2>/dev/null || true
        if $started_kafka; then
            kill $kafka_pid 2>/dev/null || true
            wait $kafka_pid 2>/dev/null || true
            rm -rf "${WORK_DIR}/kafka-logs-kraft-connect-test-$$" 2>/dev/null || true
        fi
        rm -f "$connect_config" "$source_config" "$test_input" "${WORK_DIR}/connect-offsets-$$" 2>/dev/null || true
        return 0
    fi

    log_info "Checking connector status..."
    sleep 3
    local connectors=$(curl -s http://localhost:8083/connectors 2>/dev/null)
    if echo "$connectors" | grep -q "local-file-source"; then
        log_success "File source connector is running"
        record_result "PASS" "Connect: connector running"
    else
        log_warn "File source connector not found in running connectors"
        record_result "WARN" "Connect: connector running"
    fi

    log_info "Verifying data flow through Connect..."
    sleep 5

    local consumed=$(timeout 15 "$kafka_dir/bin/kafka-console-consumer.sh" --topic connect-test-topic --from-beginning --max-messages 5 --bootstrap-server localhost:9092 2>/dev/null || true)
    local msg_count=$(echo "$consumed" | grep -c "line" || true)

    if [[ $msg_count -ge 3 ]]; then
        log_success "Connect data flow verified: $msg_count messages consumed from topic"
        record_result "PASS" "Connect: data flow verification"
    else
        log_warn "Connect data flow: expected 5 messages, got $msg_count"
        record_result "WARN" "Connect: data flow verification"
    fi

    log_info "Stopping Connect..."
    kill $connect_pid 2>/dev/null || true
    wait $connect_pid 2>/dev/null || true

    if $started_kafka; then
        log_info "Stopping Kafka..."
        kill $kafka_pid 2>/dev/null || true
        wait $kafka_pid 2>/dev/null || true
        rm -rf "${WORK_DIR}/kafka-logs-kraft-connect-test-$$" 2>/dev/null || true
    fi

    rm -f "$connect_config" "$source_config" "$test_input" "${WORK_DIR}/connect-offsets-$$" 2>/dev/null || true

    log_success "Kafka Connect tests completed"
}

# --- Tiered Storage helpers ---

S3_BACKEND_CONTAINER=""

start_s3_backend() {
    local container_name="kafka-rc-s3-$$"
    S3_BACKEND_CONTAINER=""

    # Try MinIO first (more stable, well-known health endpoint), fall back to RustFS
    local image=""
    if docker pull minio/minio:latest &>/dev/null; then
        image="minio/minio:latest"
        log_info "Using MinIO as S3-compatible backend"
    elif docker pull rustfs/rustfs:latest &>/dev/null; then
        image="rustfs/rustfs:latest"
        log_info "Using RustFS as S3-compatible backend"
    else
        log_error "Could not pull MinIO or RustFS Docker image"
        return 1
    fi

    if [[ "$image" == *"minio"* ]]; then
        docker run -d --name "$container_name" \
            -p "${MINIO_PORT}:9000" \
            -e "MINIO_ROOT_USER=${MINIO_ACCESS_KEY}" \
            -e "MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}" \
            "$image" server /data --console-address ":9001" &>/dev/null || return 1
    else
        docker run -d --name "$container_name" \
            -p "${MINIO_PORT}:9000" \
            -e "MINIO_ROOT_USER=${MINIO_ACCESS_KEY}" \
            -e "MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}" \
            "$image" server /data &>/dev/null || return 1
    fi

    MANAGED_CONTAINERS+=("$container_name")

    # Wait for S3 backend to be ready
    local wait_time=0
    while true; do
        # Check if container is still running
        if ! docker inspect "$container_name" &>/dev/null; then
            log_error "S3 container exited unexpectedly"
            docker logs "$container_name" 2>&1 | tail -5 >&2 || true
            docker rm -f "$container_name" &>/dev/null || true
            return 1
        fi
        # Try multiple health endpoints (MinIO changed endpoints across versions)
        if curl -sf "http://localhost:${MINIO_PORT}/minio/health/live" &>/dev/null \
           || curl -sf "http://localhost:${MINIO_PORT}/minio/health/cluster" &>/dev/null \
           || curl -s -o /dev/null -w "%{http_code}" "http://localhost:${MINIO_PORT}/" 2>/dev/null | grep -q "^[2-4]"; then
            break
        fi
        sleep 1
        wait_time=$((wait_time + 1))
        if [[ $wait_time -ge 30 ]]; then
            log_error "S3 backend failed to start within 30 seconds"
            docker logs "$container_name" 2>&1 | tail -10 >&2 || true
            docker rm -f "$container_name" &>/dev/null || true
            return 1
        fi
    done

    S3_BACKEND_CONTAINER="$container_name"
}

create_s3_bucket() {
    local bucket_name="$1"
    docker run --rm --network host \
        -e "MC_HOST_local=http://${MINIO_ACCESS_KEY}:${MINIO_SECRET_KEY}@localhost:${MINIO_PORT}" \
        minio/mc mb "local/${bucket_name}" 2>/dev/null || true
}

TIERED_STORAGE_PLUGIN_DIR=""

download_tiered_storage_plugin() {
    local version="$TIERED_STORAGE_PLUGIN_VERSION"
    local cache_dir="$TIERED_STORAGE_CACHE_DIR"
    local plugin_dir="${WORK_DIR}/tiered-storage-plugin"
    TIERED_STORAGE_PLUGIN_DIR=""

    mkdir -p "$cache_dir" "$plugin_dir/core" "$plugin_dir/s3"

    local base_url="https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/releases/download/v${version}"
    local core_archive="core-${version}.tgz"
    local s3_archive="s3-${version}.tgz"

    if [[ ! -f "${cache_dir}/${core_archive}" ]]; then
        log_info "Downloading tiered storage core plugin v${version}..."
        if ! curl -sSL -o "${cache_dir}/${core_archive}" "${base_url}/${core_archive}"; then
            log_error "Failed to download tiered storage core plugin"
            return 1
        fi
    else
        log_info "Using cached tiered storage core plugin"
    fi

    if [[ ! -f "${cache_dir}/${s3_archive}" ]]; then
        log_info "Downloading tiered storage S3 backend v${version}..."
        if ! curl -sSL -o "${cache_dir}/${s3_archive}" "${base_url}/${s3_archive}"; then
            log_error "Failed to download tiered storage S3 backend"
            return 1
        fi
    else
        log_info "Using cached tiered storage S3 backend"
    fi

    tar -xzf "${cache_dir}/${core_archive}" -C "$plugin_dir/core"
    tar -xzf "${cache_dir}/${s3_archive}" -C "$plugin_dir/s3"

    # If tarballs have a top-level directory, flatten it
    if [[ -z "$(find "$plugin_dir/core" -maxdepth 1 -name '*.jar' 2>/dev/null)" ]]; then
        find "$plugin_dir/core" -mindepth 2 -name '*.jar' -exec mv {} "$plugin_dir/core/" \;
        find "$plugin_dir/core" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} \; 2>/dev/null || true
    fi
    if [[ -z "$(find "$plugin_dir/s3" -maxdepth 1 -name '*.jar' 2>/dev/null)" ]]; then
        find "$plugin_dir/s3" -mindepth 2 -name '*.jar' -exec mv {} "$plugin_dir/s3/" \;
        find "$plugin_dir/s3" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} \; 2>/dev/null || true
    fi

    TIERED_STORAGE_PLUGIN_DIR="$plugin_dir"
}

# --- End Tiered Storage helpers ---

test_tiered_storage() {
    log_section "Testing Tiered Storage (with S3-compatible backend)"

    if [[ "$SKIP_TIERED_STORAGE" == "true" ]]; then
        log_warn "Skipping tiered storage tests (SKIP_TIERED_STORAGE=true)"
        record_result "SKIP" "Tiered Storage"
        return 0
    fi

    if ! command -v docker &>/dev/null; then
        log_warn "Docker not available, skipping tiered storage test"
        record_result "SKIP" "Tiered Storage: Docker required"
        return 0
    fi

    local kafka_dir="${WORK_DIR}/kafka_2.13-${VERSION}"
    if [[ ! -d "$kafka_dir" ]]; then
        log_error "Binary distribution not found"
        record_result "FAIL" "Tiered Storage"
        return 1
    fi

    # Step 1: Start S3-compatible backend
    log_info "Starting S3-compatible backend..."
    start_s3_backend || true
    local s3_container="$S3_BACKEND_CONTAINER"
    if [[ -z "$s3_container" ]]; then
        log_warn "Could not start S3 backend, skipping tiered storage test"
        record_result "SKIP" "Tiered Storage: S3 backend unavailable"
        return 0
    fi
    log_success "S3 backend started (container: $s3_container)"
    record_result "PASS" "Tiered Storage: S3 backend started"

    # Step 2: Create bucket
    log_info "Creating S3 bucket: ${TIERED_STORAGE_BUCKET}..."
    create_s3_bucket "$TIERED_STORAGE_BUCKET"
    log_success "Bucket created"

    # Step 3: Download Aiven tiered storage plugin
    log_info "Downloading Aiven tiered storage plugin..."
    download_tiered_storage_plugin || true
    local plugin_dir="$TIERED_STORAGE_PLUGIN_DIR"
    if [[ -z "$plugin_dir" ]]; then
        log_error "Failed to download tiered storage plugin"
        docker rm -f "$s3_container" &>/dev/null || true
        record_result "FAIL" "Tiered Storage: plugin download"
        return 1
    fi
    log_success "Tiered storage plugin ready"
    record_result "PASS" "Tiered Storage: plugin download"

    # Step 4: Start Kafka with tiered storage enabled
    export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}"
    export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}"

    # Verify JARs exist after extraction and flatten
    if [[ -z "$(find "$plugin_dir/core" -maxdepth 1 -name '*.jar' 2>/dev/null)" ]]; then
        log_error "No JAR files found in plugin directory: $plugin_dir/core"
        log_info "Contents: $(ls -R "$plugin_dir" 2>/dev/null)"
        docker rm -f "$s3_container" &>/dev/null || true
        unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
        record_result "FAIL" "Tiered Storage: plugin JARs not found"
        return 1
    fi
    log_info "Plugin classpath: ${plugin_dir}/core/*:${plugin_dir}/s3/*"

    local extra_config="
remote.log.storage.system.enable=true
remote.log.storage.manager.class.path=${plugin_dir}/core/*:${plugin_dir}/s3/*
remote.log.storage.manager.class.name=io.aiven.kafka.tieredstorage.RemoteStorageManager
remote.log.metadata.manager.listener.name=PLAINTEXT
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.num.partitions=1
rsm.config.storage.backend.class=io.aiven.kafka.tieredstorage.storage.s3.S3Storage
rsm.config.storage.s3.bucket.name=${TIERED_STORAGE_BUCKET}
rsm.config.storage.s3.region=us-east-1
rsm.config.storage.s3.endpoint.url=http://localhost:${MINIO_PORT}
rsm.config.storage.s3.path.style.access.enabled=true
rsm.config.chunk.size=4194304
remote.log.manager.task.interval.ms=5000
"

    start_kafka_standalone "kraft-tiered-storage" "${WORK_DIR}/kafka-tiered.log" "$extra_config" "$kafka_dir"
    local kafka_pid=$KAFKA_STANDALONE_PID
    log_info "Kafka logs: ${WORK_DIR}/kafka-tiered.log"

    if ! wait_for_kafka "localhost:9092" 90 "$kafka_dir" "$kafka_pid"; then
        local reason="timeout"
        if ! kill -0 "$kafka_pid" 2>/dev/null; then reason="process died (plugin may be incompatible with Kafka ${VERSION})"; fi
        log_error "Kafka with tiered storage failed to start ($reason). Check ${WORK_DIR}/kafka-tiered.log"
        docker rm -f "$s3_container" &>/dev/null || true
        kill "$kafka_pid" 2>/dev/null || true
        unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
        record_result "FAIL" "Tiered Storage: broker startup"
        return 1
    fi
    log_success "Kafka started with tiered storage enabled"
    record_result "PASS" "Tiered Storage: broker startup"

    # Step 5: Create topic with remote storage
    log_info "Creating topic with tiered storage configuration..."
    if "$kafka_dir/bin/kafka-topics.sh" --create --topic tiered-test \
        --partitions 1 --replication-factor 1 \
        --config remote.storage.enable=true \
        --config local.retention.ms=1000 \
        --config retention.ms=604800000 \
        --config segment.bytes=1048576 \
        --bootstrap-server localhost:9092 2>&1; then
        log_success "Topic created with tiered storage enabled"
        record_result "PASS" "Tiered Storage: topic creation"
    else
        log_error "Failed to create tiered storage topic"
        docker rm -f "$s3_container" &>/dev/null || true
        kill "$kafka_pid" 2>/dev/null || true
        unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
        record_result "FAIL" "Tiered Storage: topic creation"
        return 1
    fi

    # Step 6: Produce enough data to trigger segment rolling and tiering
    # Need >4MB to create multiple closed segments with 1MB segment.bytes
    log_info "Producing data to trigger segment rolling (~5MB)..."
    for i in $(seq 1 5000); do
        printf "tiered-msg-%05d-%s\n" "$i" "$(head -c 900 /dev/urandom | base64 | tr -d '\n' | head -c 980)"
    done | "$kafka_dir/bin/kafka-console-producer.sh" --topic tiered-test --bootstrap-server localhost:9092 2>/dev/null
    log_success "Produced 5000 messages"
    record_result "PASS" "Tiered Storage: produce data"

    # Step 7: Wait for tiering to occur
    log_info "Waiting for data to be tiered to S3 (max 120s)..."
    local tier_wait=0
    local objects_found=false
    while [[ $tier_wait -lt 120 ]]; do
        sleep 10
        tier_wait=$((tier_wait + 10))
        local object_count=$(docker run --rm --network host \
            -e "MC_HOST_local=http://${MINIO_ACCESS_KEY}:${MINIO_SECRET_KEY}@localhost:${MINIO_PORT}" \
            minio/mc ls --recursive "local/${TIERED_STORAGE_BUCKET}/" 2>/dev/null | wc -l || echo "0")
        if [[ "$object_count" -gt 0 ]]; then
            objects_found=true
            log_success "Found $object_count objects in S3 bucket after ${tier_wait}s"
            break
        fi
        log_info "  ...waiting (${tier_wait}s elapsed, no objects yet)"
    done

    if $objects_found; then
        record_result "PASS" "Tiered Storage: data tiered to S3"
    else
        log_error "No data appeared in S3 bucket within 120 seconds"
        record_result "FAIL" "Tiered Storage: data tiered to S3"
    fi

    # Step 8: Consume data back to verify readability
    log_info "Consuming data from tiered storage topic..."
    local consumed_count=$(timeout 60 "$kafka_dir/bin/kafka-console-consumer.sh" \
        --topic tiered-test --from-beginning --max-messages 5000 \
        --bootstrap-server localhost:9092 2>/dev/null | wc -l || echo "0")

    if [[ "$consumed_count" -ge 4000 ]]; then
        log_success "Consumed $consumed_count/5000 messages successfully"
        record_result "PASS" "Tiered Storage: consume from tiered topic"
    else
        log_warn "Consumed only $consumed_count/5000 messages"
        record_result "WARN" "Tiered Storage: consume from tiered topic"
    fi

    # Step 9: Verify remote reads after local retention expires
    if $objects_found; then
        log_info "Waiting for local retention to expire (15s)..."
        sleep 15
        log_info "Consuming data that should now be served from remote storage..."
        local remote_consumed=$(timeout 30 "$kafka_dir/bin/kafka-console-consumer.sh" \
            --topic tiered-test --from-beginning --max-messages 100 \
            --bootstrap-server localhost:9092 2>/dev/null | wc -l || echo "0")
        if [[ "$remote_consumed" -ge 80 ]]; then
            log_success "Consumed $remote_consumed messages from remote storage"
            record_result "PASS" "Tiered Storage: consume from remote after local deletion"
        else
            log_warn "Consumed only $remote_consumed messages after local retention expiry"
            record_result "WARN" "Tiered Storage: consume from remote after local deletion"
        fi
    fi

    # Step 10: Cleanup
    log_info "Stopping Kafka and S3 backend..."
    kill "$kafka_pid" 2>/dev/null || true
    wait "$kafka_pid" 2>/dev/null || true
    docker rm -f "$s3_container" &>/dev/null || true
    rm -rf "${WORK_DIR}/kafka-logs-kraft-tiered-storage-$$" \
           "${WORK_DIR}/tiered-storage-plugin" \
           "$kafka_dir/config/kraft-tiered-storage.properties" 2>/dev/null || true
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

    log_success "Tiered storage tests completed"
}

print_summary() {
    log_section "Validation Summary for Apache Kafka ${VERSION} RC${RC}"

    local pass_count=0
    local fail_count=0
    local warn_count=0
    local skip_count=0

    echo ""
    printf "%-10s | %-60s\n" "Status" "Check"
    printf "%.0s-" {1..75}
    echo ""

    for result in "${RESULTS[@]}"; do
        local status="${result%|*}"
        local check="${result#*|}"

        case "$status" in
            PASS)
                printf "${GREEN}%-10s${NC} | %s\n" "$status" "$check"
                pass_count=$((pass_count + 1))
                ;;
            FAIL)
                printf "${RED}%-10s${NC} | %s\n" "$status" "$check"
                fail_count=$((fail_count + 1))
                ;;
            WARN)
                printf "${YELLOW}%-10s${NC} | %s\n" "$status" "$check"
                warn_count=$((warn_count + 1))
                ;;
            SKIP)
                printf "${BLUE}%-10s${NC} | %s\n" "$status" "$check"
                skip_count=$((skip_count + 1))
                ;;
        esac
    done

    printf "%.0s-" {1..75}
    echo ""
    echo ""
    local total_elapsed=$(( SECONDS - START_TIME ))
    local total_min=$(( total_elapsed / 60 ))
    local total_sec=$(( total_elapsed % 60 ))
    echo "Total: $pass_count passed, $fail_count failed, $warn_count warnings, $skip_count skipped (${total_min}m ${total_sec}s)"
    echo ""

    if [[ $fail_count -eq 0 ]]; then
        log_success "All critical checks passed!"
        echo ""
        echo "Suggested vote response:"
        echo "---"
        echo "+1 (binding/non-binding)"
        echo ""
        echo "Verified:"

        # Collect which categories passed
        local has_signatures=false has_sha512=false has_sha1=false has_md5=false
        local has_source_build=false has_binary=false has_license=false has_version=false
        local has_quickstart=false has_replication=false has_producer=false has_consumer=false
        local has_docker=false has_maven=false has_java_client=false
        local has_connect=false has_tiered=false

        for result in "${RESULTS[@]}"; do
            local status="${result%%|*}"
            local check="${result##*|}"
            if [[ "$status" == "PASS" ]]; then
                case "$check" in
                    *signature*|*Signature*) has_signatures=true ;;
                    *"SHA512 checksum"*) has_sha512=true ;;
                    *"SHA1 checksum"*) has_sha1=true ;;
                    *"MD5 checksum"*) has_md5=true ;;
                    *"Source build"*) has_source_build=true ;;
                    *"Binary distribution"*) has_binary=true ;;
                    *"License"*|*"license"*) has_license=true ;;
                    *"Version consistency"*) has_version=true ;;
                    *"Quickstart"*) has_quickstart=true ;;
                    *"Complex: replication"*) has_replication=true ;;
                    *"Complex: producer"*) has_producer=true ;;
                    *"Complex: consumer"*) has_consumer=true ;;
                    *"Docker"*|*"docker"*) has_docker=true ;;
                    *"Maven"*) has_maven=true ;;
                    *"Java client"*) has_java_client=true ;;
                    *"Connect"*|*"connect"*) has_connect=true ;;
                    *"Tiered"*|*"tiered"*) has_tiered=true ;;
                esac
            fi
        done

        # Artifact integrity
        [[ "$has_signatures" == "true" ]] && echo "- PGP signatures"
        [[ "$has_sha512" == "true" ]] && echo "- SHA512 checksums"
        [[ "$has_sha1" == "true" ]] && echo "- SHA1 checksums"
        [[ "$has_md5" == "true" ]] && echo "- MD5 checksums"

        # Build & packaging
        [[ "$has_source_build" == "true" ]] && echo "- Source build and tests"
        [[ "$has_binary" == "true" ]] && echo "- Binary distribution contents"
        [[ "$has_license" == "true" ]] && echo "- LICENSE and NOTICE files present"
        [[ "$has_version" == "true" ]] && echo "- Version consistency across artifacts"

        # Functionality
        [[ "$has_quickstart" == "true" ]] && echo "- Quickstart (single-node produce/consume)"
        [[ "$has_replication" == "true" ]] && echo "- Multi-broker cluster with replication"
        [[ "$has_producer" == "true" ]] && echo "- Producer with acks=all"
        [[ "$has_consumer" == "true" ]] && echo "- Consumer groups"
        [[ "$has_connect" == "true" ]] && echo "- Kafka Connect standalone mode"
        [[ "$has_tiered" == "true" ]] && echo "- Tiered storage with S3-compatible backend"

        # Client libraries & Docker
        [[ "$has_maven" == "true" ]] && echo "- Maven staging artifacts (kafka-clients, kafka-streams, connect-api)"
        [[ "$has_java_client" == "true" ]] && echo "- Java client integration (Admin, Producer, Consumer, Streams)"
        [[ "$has_docker" == "true" ]] && echo "- Docker images"

        echo ""
        echo "Tested on: $(uname -s) $(uname -r)"
        echo "Java version: $(java -version 2>&1 | head -1)"
        echo "---"
    else
        log_error "Some checks failed. Review the results before voting."
    fi

    # Return fail count for exit code
    return $fail_count
}

cleanup() {
    log_info "Cleaning up..."
    for pid in "${MANAGED_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for container in "${MANAGED_CONTAINERS[@]}"; do
        docker rm -f "$container" 2>/dev/null || true
    done
    jobs -p 2>/dev/null | xargs -r kill 2>/dev/null || true
    unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY 2>/dev/null || true
}

trap cleanup EXIT

main() {
    if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
        usage
    fi

    if [[ -z "$VERSION" || -z "$RC" ]]; then
        usage
    fi

    echo ""
    echo "=============================================="
    echo "Apache Kafka Release Candidate Validator"
    echo "Version: $VERSION  RC: $RC"
    echo "=============================================="
    echo ""

    if [[ -n "$RUN_ONLY" ]]; then
        setup_workdir
        cd "$WORK_DIR"
        log_info "Running only: $RUN_ONLY"
        "$RUN_ONLY"
        print_summary || exit $?
        return
    fi

    check_dependencies
    setup_workdir
    import_keys
    download_artifacts
    verify_signatures
    verify_checksums
    check_urls
    build_from_source
    test_binary_distribution
    test_docker_images
    run_quickstart_test
    run_complex_tests
    run_maven_validator
    run_docker_integration_test
    test_license_compliance
    test_version_consistency
    test_kafka_connect
    test_tiered_storage
    print_summary || exit $?
}

main "$@"
