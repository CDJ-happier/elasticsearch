#!/bin/bash

# Elasticsearch Test Data Generator
# Generates and inserts test data into Elasticsearch clusters managed by es-cluster-manager.sh

set -euo pipefail

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ES_BASE_DIR="$(cd "${SCRIPT_DIR}/../dev-utils" && pwd)"
CONFIG_DIR="${ES_BASE_DIR}/config"
CLUSTERS_CONFIG_DIR="${CONFIG_DIR}/clusters"

# Default settings
DEFAULT_INDEX_NAME="test-data"
DEFAULT_DOC_COUNT=1000
DEFAULT_BATCH_SIZE=100

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${CYAN}[DEBUG]${NC} $1"
}

# Help function
help() {
    cat << EOF
Elasticsearch Test Data Generator

Usage: $0 [cluster_name] [options]

Arguments:
  cluster_name             Name of the cluster to generate data for (default: first available cluster)

Options:
  -i, --index NAME         Index name to insert data into (default: test-data)
  -c, --count COUNT        Number of documents to generate (default: 1000)
  -b, --batch-size SIZE    Number of documents per batch request (default: 100)
  -H, --host HOST          Elasticsearch host (default: determined from cluster config)
  -p, --port PORT          Elasticsearch port (default: determined from cluster config)
  -u, --user USER          Username for authentication (if security enabled)
  -P, --password PASS      Password for authentication (if security enabled)
  --clean                  Delete index before inserting data
  -v, --verbose            Verbose output
  -h, --help               Show this help

Examples:
  ./generate-test-data.sh                            # Generate data for first available cluster
  ./generate-test-data.sh my-cluster                 # Generate data for specific cluster
  ./generate-test-data.sh my-cluster -c 5000 -b 200  # Generate 5000 docs in batches of 200
  ./generate-test-data.sh my-cluster --clean         # Delete index and insert fresh data
  ./generate-test-data.sh -H localhost -p 9200       # Generate data for external cluster
EOF
}

# Parse command line arguments
parse_args() {
    local args=()
    while [[ $# -gt 0 ]]; do
        case $1 in
            -i|--index)
                INDEX_NAME="$2"
                shift 2
                ;;
            -c|--count)
                DOC_COUNT="$2"
                shift 2
                ;;
            -b|--batch-size)
                BATCH_SIZE="$2"
                shift 2
                ;;
            -H|--host)
                ES_HOST="$2"
                shift 2
                ;;
            -p|--port)
                ES_PORT="$2"
                shift 2
                ;;
            -u|--user)
                ES_USER="$2"
                shift 2
                ;;
            -P|--password)
                ES_PASSWORD="$2"
                shift 2
                ;;
            --clean)
                CLEAN_INDEX=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                help
                exit 0
                ;;
            -*)
                log_error "Unknown option $1"
                help
                exit 1
                ;;
            *)
                args+=("$1")
                shift
                ;;
        esac
    done

    # Set cluster name from positional arguments
    if [ ${#args[@]} -gt 0 ]; then
        CLUSTER_NAME="${args[0]}"
    fi

    # Set defaults if not provided
    INDEX_NAME="${INDEX_NAME:-$DEFAULT_INDEX_NAME}"
    DOC_COUNT="${DOC_COUNT:-$DEFAULT_DOC_COUNT}"
    BATCH_SIZE="${BATCH_SIZE:-$DEFAULT_BATCH_SIZE}"
    CLEAN_INDEX="${CLEAN_INDEX:-false}"
    VERBOSE="${VERBOSE:-false}"
}

# Get cluster configuration
get_cluster_config() {
    local cluster_name="$1"
    local key="$2"
    local default="$3"
    local config_file="${CLUSTERS_CONFIG_DIR}/${cluster_name}/cluster.yml"

    if [ -f "$config_file" ]; then
        local value=$(grep "^${key}:" "$config_file" | cut -d':' -f2- | sed 's/^ *//g' | sed 's/ *$//g' | sed 's/^"//g' | sed 's/"$//g')
        echo "${value:-$default}"
    else
        echo "$default"
    fi
}

# Get node configuration
get_node_config() {
    local cluster_name="$1"
    local node_index="$2"
    local key="$3"
    local default="$4"
    local config_file="${CLUSTERS_CONFIG_DIR}/${cluster_name}/${cluster_name}-node${node_index}.yml"

    if [ -f "$config_file" ]; then
        local value=$(grep "^${key}:" "$config_file" | cut -d':' -f2- | sed 's/^ *//g' | sed 's/ *$//g' | sed 's/^"//g' | sed 's/"$//g')
        echo "${value:-$default}"
    else
        echo "$default"
    fi
}

# Find available clusters
find_clusters() {
    if [ -d "$CLUSTERS_CONFIG_DIR" ]; then
        find "$CLUSTERS_CONFIG_DIR" -maxdepth 1 -mindepth 1 -type d -exec basename {} \;
    fi
}

# Determine cluster connection details
determine_cluster_connection() {
    # If host and port are already provided, use them
    if [[ -n "${ES_HOST:-}" && -n "${ES_PORT:-}" ]]; then
        log_info "Using provided host: $ES_HOST and port: $ES_PORT"
        return
    fi

    # If no cluster name provided, use the first available cluster
    if [ -z "${CLUSTER_NAME:-}" ]; then
        local clusters=($(find_clusters))
        if [ ${#clusters[@]} -eq 0 ]; then
            log_error "No clusters found and no host/port specified"
            exit 1
        fi
        CLUSTER_NAME="${clusters[0]}"
        log_info "Using cluster: $CLUSTER_NAME"
    fi

    # Check if cluster exists
    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${CLUSTER_NAME}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$CLUSTER_NAME' not found"
        exit 1
    fi

    # Get cluster configuration
    local node_count=$(get_cluster_config "$CLUSTER_NAME" "cluster.node_count" "1")

    # Use first node for connection
    ES_PORT=$(get_node_config "$CLUSTER_NAME" "1" "http.port" "9200")
    ES_HOST="localhost"

    log_info "Determined connection - Host: $ES_HOST, Port: $ES_PORT"
}

# Check if Elasticsearch is available
check_elasticsearch() {
    local auth_param=""
    if [[ -n "${ES_USER:-}" && -n "${ES_PASSWORD:-}" ]]; then
        auth_param="--user ${ES_USER}:${ES_PASSWORD}"
    fi

    if curl -s -f ${auth_param:-} "http://${ES_HOST}:${ES_PORT}/_cluster/health" > /dev/null; then
        log_success "Connected to Elasticsearch at ${ES_HOST}:${ES_PORT}"
        return 0
    else
        log_error "Cannot connect to Elasticsearch at ${ES_HOST}:${ES_PORT}"
        return 1
    fi
}

# Clean index if requested
clean_index() {
    if [ "$CLEAN_INDEX" = true ]; then
        log_info "Deleting index '$INDEX_NAME' if it exists"
        local auth_param=""
        if [[ -n "${ES_USER:-}" && -n "${ES_PASSWORD:-}" ]]; then
            auth_param="--user ${ES_USER}:${ES_PASSWORD}"
        fi

        curl -s -X DELETE ${auth_param:-} "http://${ES_HOST}:${ES_PORT}/${INDEX_NAME}" > /dev/null 2>&1 || true
        sleep 1
    fi
}

# Generate test data
generate_test_data() {
    log_info "Generating $DOC_COUNT documents in batches of $BATCH_SIZE"

    local auth_param=""
    if [[ -n "${ES_USER:-}" && -n "${ES_PASSWORD:-}" ]]; then
        auth_param="--user ${ES_USER}:${ES_PASSWORD}"
    fi

    # Create index
    log_info "Creating index '$INDEX_NAME'"
    curl -s -X PUT ${auth_param:-} \
        -H "Content-Type: application/json" \
        -d '{
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "name": { "type": "text" },
                    "email": { "type": "keyword" },
                    "age": { "type": "integer" },
                    "balance": { "type": "float" },
                    "created": { "type": "date" },
                    "isActive": { "type": "boolean" },
                    "tags": { "type": "keyword" },
                    "location": { "type": "geo_point" }
                }
            }
        }' \
        "http://${ES_HOST}:${ES_PORT}/${INDEX_NAME}" > /dev/null || true

    sleep 1

    # Generate and insert documents
    local inserted=0
    while [ $inserted -lt $DOC_COUNT ]; do
        local batch_end=$((inserted + BATCH_SIZE))
        if [ $batch_end -gt $DOC_COUNT ]; then
            batch_end=$DOC_COUNT
        fi

        local batch_size=$((batch_end - inserted))
        log_info "Inserting batch: $((inserted + 1))-$batch_end"

        # Create bulk request
        local bulk_data=""
        for ((i=1; i<=batch_size; i++)); do
            local id=$((inserted + i))
            local name="User $id"
            local email="user$id@example.com"
            local age=$((20 + RANDOM % 50))
            local balance=$((1000 + RANDOM % 9000)).$((RANDOM % 100))
            local created="202$(($RANDOM % 5))-$(printf "%02d" $((1 + RANDOM % 12)))-$(printf "%02d" $((1 + RANDOM % 28)))"
            local isActive=$([ $((RANDOM % 2)) -eq 0 ] && echo "true" || echo "false")
            local tag1="tag$((RANDOM % 10))"
            local tag2="tag$((RANDOM % 10 + 10))"
            local lat=$((40 + RANDOM % 5)).$((RANDOM % 1000000))
            local lon=$((-74 - RANDOM % 5)).$((RANDOM % 1000000))

            bulk_data+="{\"index\":{\"_id\":\"$id\"}}\n"
            bulk_data+="{\"name\":\"$name\",\"email\":\"$email\",\"age\":$age,\"balance\":$balance,\"created\":\"$created\",\"isActive\":$isActive,\"tags\":[\"$tag1\",\"$tag2\"],\"location\":\"$lat,$lon\"}\n"
        done

        # Debug output in verbose mode
        if [ "$VERBOSE" = true ]; then
            log_debug "Bulk request data (first 2 lines):"
            echo -e "$bulk_data" | head -n 2 | while IFS= read -r line; do
                log_debug "  $line"
            done
        fi

        # Save bulk data to temp file for curl
        local bulk_file=$(mktemp)
        echo -e "$bulk_data" > "$bulk_file"

        # Send bulk request
        local response_file=$(mktemp)
        local http_code=$(curl -s -w "%{http_code}" -o "$response_file" \
            -X POST ${auth_param:-} \
            -H "Content-Type: application/json" \
            --data-binary "@$bulk_file" \
            "http://${ES_HOST}:${ES_PORT}/${INDEX_NAME}/_bulk")

        # Clean up temp files
        rm "$bulk_file"

        if [ "$http_code" != "200" ]; then
            log_error "Failed to insert batch. HTTP code: $http_code"
            if [ "$VERBOSE" = true ]; then
                log_debug "Response: $(cat "$response_file")"
            fi
            rm "$response_file"
            exit 1
        fi

        # Check for errors in response
        if grep -q '"errors":true' "$response_file"; then
            log_error "Bulk request contained errors"
            if [ "$VERBOSE" = true ]; then
                log_debug "Response: $(cat "$response_file" | head -n 20)"
            fi
            rm "$response_file"
            exit 1
        fi

        rm "$response_file"
        inserted=$batch_end
    done

    log_success "Successfully inserted $DOC_COUNT documents into index '$INDEX_NAME'"
}

# Main function
main() {
    parse_args "$@"

    determine_cluster_connection

    check_elasticsearch

    clean_index

    generate_test_data
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
