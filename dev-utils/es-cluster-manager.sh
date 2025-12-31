#!/bin/bash

# Enhanced Elasticsearch Cluster Manager
# Unified script for managing single and multi-cluster deployments
# Configuration-driven approach with YAML-based setup
# Supports easy cluster/node addition and flexible JVM configuration

set -euo pipefail

# Default configuration
# by executing 'sudo ln -sf /data1/elk/elasticsearch/dev-utils/es-cluster-manager.sh /usr/local/bin/esctl'
# we can use 'esctl' to manage the cluster
# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_DIR="/data1/elk/elasticsearch/dev-utils"
ES_INSTALL_DIR="$(cd "${SCRIPT_DIR}/../build/distribution/local/elasticsearch-8.17.11-SNAPSHOT" && pwd)"
ES_BASE_DIR="$(cd "${SCRIPT_DIR}/../dev-utils" && pwd)"
CONFIG_DIR="${ES_BASE_DIR}/config"
CLUSTERS_CONFIG_DIR="${CONFIG_DIR}/clusters"

# Default settings
DEFAULT_JVM_HEAP="2g"
DEFAULT_BASE_HTTP_PORT=31920
DEFAULT_BASE_TRANSPORT_PORT=9300
DEFAULT_BASE_DEBUG_PORT=5005
DEFAULT_NODE_COUNT=3
DEFAULT_CLUSTER_PREFIX="es-cluster"
DEFAULT_TEMPLATE="basic"

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
Enhanced Elasticsearch Cluster Manager

Usage: $0 <command> [cluster_name] [options]

Commands:
  init <cluster_name>     Initialize a new cluster configuration
  start <cluster_name>    Start a cluster or specific nodes
  stop <cluster_name>     Stop a cluster or specific nodes
  restart <cluster_name>  Restart a cluster
  status <cluster_name>   Check cluster status
  list                    List all configured clusters
  add-node <cluster_name> Add a new node to existing cluster
  remove-node <cluster_name> <node_name> Remove a node from cluster
  cleanup <cluster_name>  Clean up cluster data and logs
  logs <cluster_name> [node_name] Show cluster logs
  config <cluster_name>   Show cluster configuration
  setup-security <cluster_name> Setup SSL certificates for cluster
  set-password <cluster_name> Set password for elastic user

Options:
  -n, --nodes COUNT       Number of nodes (default: 3)
  -t, --template TYPE     Configuration template (basic|dev|test|production)
  -j, --jvm-heap SIZE     JVM heap size (e.g., 512m, 1g, 2g)
  -p, --http-port PORT    Starting HTTP port (default: 9200)
  -s, --security          Enable security features
  -f, --force             Force operation
  -v, --verbose           Verbose output
  -h, --help              Show this help

Security Setup:
  esctl init <cluster_name> doesn't support security by default, you need to set it manually if you want to enable it.
  Specifically, you need to set xpack.security.enabled and xpack.security.transport.ssl.enabled to true
  in the elasticsearch.yml file. AND THEN:
  When xpack.security.enabled is true, you need to:
  1. Setup SSL certificates: ./es-cluster-manager.sh setup-security <cluster_name>
  2. Start the cluster: ./es-cluster-manager.sh start <cluster_name>
  3. Set password: ./es-cluster-manager.sh set-password <cluster_name> [password]
     - If password is not provided, a random password will be generated
     - If password is provided, it will be set as the elastic user password

Examples:
  ./es-cluster-manager.sh init my-cluster -n 3 -t dev -j 1g
  ./es-cluster-manager.sh start my-cluster
  ./es-cluster-manager.sh start my-cluster --nodes 1,2  # Start only nodes 1 and 2
  ./es-cluster-manager.sh stop my-cluster
  ./es-cluster-manager.sh stop my-cluster --nodes 1,2  # Stop only nodes 1 and 2
  ./es-cluster-manager.sh stop my-cluster --force
  ./es-cluster-manager.sh status my-cluster
  ./es-cluster-manager.sh add-node my-cluster
  ./es-cluster-manager.sh list
  ./es-cluster-manager.sh setup-security my-cluster
  ./es-cluster-manager.sh set-password my-cluster        # Generate random password
  ./es-cluster-manager.sh set-password my-cluster elastic # Set password to 'elastic'
EOF
}

# Utility functions
check_dependencies() {
    if [ ! -d "$ES_INSTALL_DIR" ]; then
        log_error "Elasticsearch installation not found at $ES_INSTALL_DIR"
        exit 1
    fi

    if [ ! -x "$ES_INSTALL_DIR/bin/elasticsearch" ]; then
        log_error "Elasticsearch binary not executable at $ES_INSTALL_DIR/bin/elasticsearch"
        exit 1
    fi
}

# Calculate port based on cluster and node
calculate_ports() {
    local cluster_name="$1"
    local node_index="$2"
    local base_http_port="${3:-$DEFAULT_BASE_HTTP_PORT}"
    local base_transport_port="${4:-$DEFAULT_BASE_TRANSPORT_PORT}"
    local base_debug_port="${5:-$DEFAULT_BASE_DEBUG_PORT}"

    # Use cluster offset to avoid port conflicts
#    local cluster_hash=$(echo -n "$cluster_name" | cksum | cut -d' ' -f1)
#    local cluster_offset=$((cluster_hash % 100))
#    local http_port=$((base_http_port + cluster_offset * 10 + node_index - 1))
#    local transport_port=$((base_transport_port + cluster_offset * 10 + node_index - 1))
    local http_port=$((base_http_port + node_index - 1))
    local transport_port=$((base_transport_port + node_index - 1))
    local debug_port=$((base_debug_port + node_index - 1))

    echo "$http_port:$transport_port:$debug_port"
}

# Node naming convention
get_node_name() {
    local cluster_name="$1"
    local node_index="$2"
    echo "${cluster_name}-node${node_index}"
}

# Get node configuration value from YAML file
get_node_config() {
    local cluster_name="$1"
    local node_name="$2"
    local config_key="$3"
    local default_value="$4"

    local node_config_file="${CLUSTERS_CONFIG_DIR}/${cluster_name}/${node_name}.yml"

    if [ ! -f "$node_config_file" ]; then
        echo "$default_value"
        return
    fi

    # Extract value from YAML config file
    local value=$(grep "^$config_key:" "$node_config_file" | sed -e 's/[^:]*://;s/^[[:space:]]*//;s/[[:space:]]*$//')

    # If value not found, return default
    if [ -z "$value" ]; then
        echo "$default_value"
    else
        echo "$value"
    fi
}

# Directory management
ensure_directories() {
    local cluster_name="$1"
    local node_name="$2"

    local data_dir="${ES_BASE_DIR}/data/${cluster_name}/${node_name}"
    local logs_dir="${ES_BASE_DIR}/logs/${cluster_name}/${node_name}"
    local pid_dir="${ES_BASE_DIR}/pids"

    mkdir -p "$data_dir" "$logs_dir" "$pid_dir"
}

# Configuration generation
generate_node_config() {
    local cluster_name="$1"
    local node_index="$2"
    local node_count="$3"
    local http_port="$4"
    local transport_port="$5"
    local jvm_heap="$6"
    local cluster_dir="$7"
    local debug_port="$8"

    local node_name=$(get_node_name "$cluster_name" "$node_index")
    local config_file="${CLUSTERS_CONFIG_DIR}/${cluster_name}/${node_name}.yml"

    cat > "$config_file" << EOF
# Configuration for ${node_name}
cluster.name: ${cluster_name}
node.name: ${node_name}
node.roles: [master, data, ingest]

# Network settings
http.port: ${http_port}
transport.port: ${transport_port}

# Path settings
path.data: ${ES_BASE_DIR}/data/${cluster_name}/${node_name}
path.logs: ${ES_BASE_DIR}/logs/${cluster_name}/${node_name}
EOF

    # Generate JVM configuration for this node
    local jvm_config_file="${CLUSTERS_CONFIG_DIR}/${cluster_name}/${node_name}.jvm.options"
    cat > "$jvm_config_file" << EOF
# JVM configuration for ${node_name}
-Xms${jvm_heap}
-Xmx${jvm_heap}

# Memory dump settings
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=${ES_BASE_DIR}/logs/${cluster_name}/${node_name}

# GC logging
-Xlog:gc*,gc+age=trace,safepoint:file=${ES_BASE_DIR}/logs/${cluster_name}/${node_name}/gc.log:utctime,pid,tags:filecount=32,filesize=64m
# for debugging
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=${debug_port}
EOF
}

# Generate default JVM configuration
generate_default_jvm_config() {
    local cluster_name="$1"
    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    local jvm_config_file="${cluster_dir}/jvm.options"

    cat > "$jvm_config_file" << EOF
# Default JVM configuration for cluster ${cluster_name}
# G1GC settings
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:+DisableExplicitGC
-XX:+AlwaysPreTouch

# Basic settings
-Xss1m
-Djava.awt.headless=true
-Dfile.encoding=UTF-8
-Djna.nosys=true
-Dio.netty.noUnsafe=true
-Dio.netty.noKeySetOptimization=true
-Dio.netty.recycler.maxCapacityPerThread=0
-Dlog4j.shutdownHookEnabled=false
-Dlog4j2.disable.jmx=true
-Djava.io.tmpdir=\${ES_TMPDIR}
EOF
}

# Cluster initialization
init_cluster() {
    local cluster_name="$1"
    local node_count="${2:-$DEFAULT_NODE_COUNT}"
    local template="${3:-basic}"
    local jvm_heap="${4:-$DEFAULT_JVM_HEAP}"
    local base_http_port="${5:-$DEFAULT_BASE_HTTP_PORT}"
    local enable_security="${6:-false}"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"

    if [ -d "$cluster_dir" ]; then
        if [ "${FORCE:-false}" != "true" ]; then
            log_warning "Cluster '$cluster_name' already exists. Use --force to overwrite."
            return 1
        fi
        log_warning "Overwriting existing cluster '$cluster_name'"
        rm -rf "$cluster_dir"
    fi

    mkdir -p "$cluster_dir"

    # Build seed hosts for all nodes
    local seed_hosts=""
    local master_nodes=""
    for i in $(seq 1 "$node_count"); do
        local ports=$(calculate_ports "$cluster_name" "$i" "$base_http_port" "$DEFAULT_BASE_TRANSPORT_PORT" "$DEFAULT_BASE_DEBUG_PORT")
        local transport_port=$(echo "$ports" | cut -d':' -f2)
        seed_hosts="${seed_hosts}\"127.0.0.1:$transport_port\""
        master_nodes="${master_nodes}\"$(get_node_name "$cluster_name" "$i")\""
        if [ $i -lt $node_count ]; then
            seed_hosts="${seed_hosts}, "
            master_nodes="${master_nodes}, "
        fi
    done

    # Create Elasticsearch configuration (common settings)
    cat > "${cluster_dir}/elasticsearch.yml" << EOF
# Elasticsearch configuration for ${cluster_name}
cluster.name: ${cluster_name}
# network settings
network.host: 0.0.0.0
# xpack settings
xpack.security.enabled: ${enable_security}
xpack.security.enrollment.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false
# allow CORS request form http://localhost:5173
http.cors.enabled: true
http.cors.allow-origin: "http://localhost:5173"

# Discovery settings
discovery.seed_hosts: [${seed_hosts}]
cluster.initial_master_nodes: [${master_nodes}]
EOF

    # Copy necessary configuration files
    cp "${ES_INSTALL_DIR}/config/log4j2.properties" "${cluster_dir}/"

    # Generate default JVM configuration (required by Elasticsearch)
    generate_default_jvm_config "$cluster_name"

    # Generate node configurations
    for i in $(seq 1 "$node_count"); do
        local ports=$(calculate_ports "$cluster_name" "$i" "$base_http_port" "$DEFAULT_BASE_TRANSPORT_PORT" "$DEFAULT_BASE_DEBUG_PORT")
        local http_port=$(echo "$ports" | cut -d':' -f1)
        local transport_port=$(echo "$ports" | cut -d':' -f2)
        local debug_port=$(echo "$ports" | cut -d':' -f3)

        generate_node_config "$cluster_name" "$i" "$node_count" "$http_port" "$transport_port" "$jvm_heap" "$cluster_dir" "$debug_port"
    done

    log_success "Cluster '$cluster_name' initialized with $node_count nodes"
    log_info "Configuration directory: $cluster_dir"
}

# Start cluster
start_cluster() {
    local cluster_name="$1"
    local specific_nodes="${2:-}"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found. Run init first."
        exit 1
    fi

    # Count nodes by checking node config files
    local node_count=0
    for node_config in "${cluster_dir}"/*.yml; do
        if [[ "$(basename "$node_config")" =~ ^${cluster_name}-node[0-9]+\.yml$ ]]; then
            node_count=$((node_count + 1))
        fi
    done

    log_info "Starting cluster '$cluster_name'..."

    # Create PID file
    local pid_file="${ES_BASE_DIR}/pids/${cluster_name}.pids"
    mkdir -p "$(dirname "$pid_file")"

    # Determine which nodes to start
    local nodes_to_start=()
    if [ -n "$specific_nodes" ]; then
        IFS=',' read -ra nodes_to_start <<< "$specific_nodes"
    else
        for i in $(seq 1 "$node_count"); do
            nodes_to_start+=("$i")
        done
    fi

    # Start each node
    for node_index in "${nodes_to_start[@]}"; do
        if [ "$node_index" -gt "$node_count" ]; then
            log_warning "Node $node_index does not exist (max: $node_count)"
            continue
        fi

        local node_name=$(get_node_name "$cluster_name" "$node_index")
        local node_config_file="${cluster_dir}/${node_name}.yml"

        if [ ! -f "$node_config_file" ]; then
            log_warning "Node config file $node_config_file not found"
            continue
        fi

        # Check if already running
        if pgrep -f "cluster.name=${cluster_name}.*node.name=${node_name}" > /dev/null; then
            log_info "Node $node_name is already running"
            continue
        fi

        ensure_directories "$cluster_name" "$node_name"

        # Clean old lock files
        rm -f "${ES_BASE_DIR}/data/${cluster_name}/${node_name}/node.lock" 2>/dev/null || true

        log_info "Starting node $node_name..."

        # Read node configuration
        local http_port=$(get_node_config "$cluster_name" "$node_name" "http.port" "9200")
        local transport_port=$(get_node_config "$cluster_name" "$node_name" "transport.port" "9300")

        # Set environment variables
        export ES_PATH_CONF="${cluster_dir}"
        export ES_JAVA_HOME="${ES_JAVA_HOME:-${JAVA_HOME:-/usr}}"

        # Build JVM options from node-specific JVM config file
        local jvm_options_file="${cluster_dir}/${node_name}.jvm.options"
        local jvm_args=""
        if [ -f "$jvm_options_file" ]; then
            jvm_args=$(grep -v '^#' "$jvm_options_file" | tr '\n' ' ')
        fi

        # Start Elasticsearch with proper configuration
        cd "$ES_INSTALL_DIR"

        if [ "${VERBOSE:-false}" = "true" ]; then
            log_debug "Starting with JVM args: $jvm_args"
            log_debug "HTTP port: $http_port, Transport port: $transport_port"
        fi

        ES_JAVA_OPTS="$jvm_args" \
        ES_PATH_CONF="${cluster_dir}" \
        nohup "$ES_INSTALL_DIR/bin/elasticsearch" \
            -E "path.data=${ES_BASE_DIR}/data/${cluster_name}/${node_name}" \
            -E "path.logs=${ES_BASE_DIR}/logs/${cluster_name}/${node_name}" \
            -E "node.name=${node_name}" \
            -E "cluster.name=${cluster_name}" \
            -E "http.port=${http_port}" \
            -E "transport.port=${transport_port}" \
            > "${ES_BASE_DIR}/logs/${cluster_name}/${node_name}/stdout.log" \
            2> "${ES_BASE_DIR}/logs/${cluster_name}/${node_name}/stderr.log" &

        local pid=$!
        echo "$pid:$node_name:$http_port" >> "$pid_file"

        # Wait for startup
        sleep 3

        if kill -0 "$pid" 2>/dev/null; then
            log_success "Node $node_name started (PID: $pid, HTTP: $http_port)"
        else
            log_error "Failed to start node $node_name"
            log_error "Check logs: ${ES_BASE_DIR}/logs/${cluster_name}/${node_name}/stderr.log"
            exit 1
        fi
    done

    log_success "Cluster '$cluster_name' started successfully"
}

# Stop cluster
stop_cluster() {
    local cluster_name="$1"
    local specific_nodes="${2:-}"
    local force="${3:-false}"

    local pid_file="${ES_BASE_DIR}/pids/${cluster_name}.pids"
    if [ ! -f "$pid_file" ]; then
        log_warning "No PID file found for cluster '$cluster_name'"
        return 0
    fi

    log_info "Stopping cluster '$cluster_name'..."

    local stopped_count=0
    local total_count=0
    local filtered_count=0

    # Build list of nodes to stop
    local nodes_to_stop=()
    if [ -n "$specific_nodes" ]; then
        IFS=',' read -ra nodes_to_stop <<< "$specific_nodes"
    fi

    # First, count total nodes in PID file
    while IFS=':' read -r pid node_name http_port; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            total_count=$((total_count + 1))
        fi
    done < "$pid_file"

    # Reset file pointer and process nodes
    while IFS=':' read -r pid node_name http_port; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            # Check if we should stop this node
            if [ -n "$specific_nodes" ]; then
                local should_stop=false
                for node in "${nodes_to_stop[@]}"; do
                    if [[ "$node_name" == *"node${node}" ]]; then
                        should_stop=true
                        break
                    fi
                done
                [ "$should_stop" = false ] && continue
            fi

            log_info "Stopping node $node_name (PID: $pid)..."

            if [ "$force" = "true" ]; then
                kill -9 "$pid" 2>/dev/null || true
                log_success "Forcefully stopped $node_name"
                stopped_count=$((stopped_count + 1))
            else
                kill -TERM "$pid"

                # Wait for graceful shutdown
                local stopped=false
                for i in {1..30}; do
                    if ! kill -0 "$pid" 2>/dev/null; then
                        log_success "Gracefully stopped $node_name"
                        stopped_count=$((stopped_count + 1))
                        stopped=true
                        break
                    fi
                    sleep 1
                done

                if [ "$stopped" = false ]; then
                    log_warning "$node_name did not stop gracefully, forcefully killing..."
                    kill -9 "$pid" 2>/dev/null || true
                    stopped_count=$((stopped_count + 1))
                fi
            fi
        fi
    done < "$pid_file"

    # Update PID file when stopping specific nodes
    if [ -n "$specific_nodes" ] && [ "$stopped_count" -gt 0 ]; then
        # Create temporary file to store running nodes
        local temp_pid_file="${pid_file}.tmp"
        > "$temp_pid_file"

        # Read original PID file and keep only running nodes
        while IFS=':' read -r pid node_name http_port; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "${pid}:${node_name}:${http_port}" >> "$temp_pid_file"
            fi
        done < "$pid_file"

        # Replace original PID file with updated one
        mv "$temp_pid_file" "$pid_file"
    fi

    # Clean up PID file only when stopping all nodes
    if [ -z "$specific_nodes" ] && [ "$stopped_count" -eq "$total_count" ]; then
        rm -f "$pid_file"
    fi

    log_success "Stopped $stopped_count/$filtered_count nodes for cluster '$cluster_name'"
}

# Check cluster status
status_cluster() {
    local cluster_name="$1"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    # Count nodes by checking node config files
    local node_count=0
    for node_config in "${cluster_dir}"/*.yml; do
        if [[ "$(basename "$node_config")" =~ ^${cluster_name}-node[0-9]+\.yml$ ]]; then
            node_count=$((node_count + 1))
        fi
    done

    log_info "=== Cluster Status: $cluster_name ==="

    # Check running processes
    local pid_file="${ES_BASE_DIR}/pids/${cluster_name}.pids"
    local running_nodes=0
    local node_info=()

    if [ -f "$pid_file" ]; then
        while IFS=':' read -r pid node_name http_port; do
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                running_nodes=$((running_nodes + 1))
                node_info+=("$node_name:$http_port:$pid")
            fi
        done < "$pid_file"
    fi

    log_info "Running nodes: $running_nodes/$node_count"

    # Check cluster health
    local healthy=false
    if [ ${#node_info[@]} -gt 0 ]; then
        for node_info_line in "${node_info[@]}"; do
            local http_port=$(echo "$node_info_line" | cut -d':' -f2)

            # Try without authentication first
            local response=$(curl -s -w "\n%{http_code}" "http://localhost:$http_port/_cluster/health" 2>/dev/null)
            local http_code=$(echo "$response" | tail -n1)

            if [ "$http_code" = "200" ]; then
                log_info "Cluster responding on port $http_port"
                local health=$(echo "$response" | head -n-1 | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
                log_info "Cluster health: $health"
                healthy=true

                # Show node details
                log_info "Node details:"
                curl -s "http://localhost:$http_port/_cat/nodes?v" 2>/dev/null || true
                break
            elif [ "$http_code" = "401" ]; then
                # Try with elastic:cdjcdj credentials. use password generated by esctl set-password xxx
                response=$(curl -s -w "\n%{http_code}" --user "elastic:cdjcdj" "http://localhost:$http_port/_cluster/health" 2>/dev/null)
                http_code=$(echo "$response" | tail -n1)

                if [ "$http_code" = "200" ]; then
                    log_info "Cluster responding on port $http_port (authenticated with elastic:cdjcdj)"
                    local health=$(echo "$response" | head -n-1 | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
                    log_info "Cluster health: $health"
                    healthy=true

                    # Show node details with authentication
                    log_info "Node details:"
                    curl -s --user "elastic:cdjcdj" "http://localhost:$http_port/_cat/nodes?v" 2>/dev/null || true
                    break
                else
                    # Try with elastic:changeme credentials
                    response=$(curl -s -w "\n%{http_code}" --user "elastic:changeme" "http://localhost:$http_port/_cluster/health" 2>/dev/null)
                    http_code=$(echo "$response" | tail -n1)

                    if [ "$http_code" = "200" ]; then
                        log_info "Cluster responding on port $http_port (authenticated with elastic:changeme)"
                        local health=$(echo "$response" | head -n-1 | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
                        log_info "Cluster health: $health"
                        healthy=true

                        # Show node details with authentication
                        log_info "Node details:"
                        curl -s --user "elastic:changeme" "http://localhost:$http_port/_cat/nodes?v" 2>/dev/null || true
                        break
                    else
                        log_warning "Cluster requires authentication but default passwords don't work"
                        log_info "Please provide correct credentials"
                    fi
                fi
            fi
        done
    fi

    if [ "$healthy" = false ] && [ "$running_nodes" -gt 0 ]; then
        log_warning "Cluster nodes running but not responding on expected ports"
        log_info "Running nodes:"
        for node_info_line in "${node_info[@]}"; do
            local node_name=$(echo "$node_info_line" | cut -d':' -f1)
            local http_port=$(echo "$node_info_line" | cut -d':' -f2)
            local pid=$(echo "$node_info_line" | cut -d':' -f3)
            log_info "  $node_name: http://localhost:$http_port (PID: $pid)"
        done
    fi

    # Show configuration
    log_info "Configuration:"
    log_info "  Cluster name: $cluster_name"
    log_info "  Node count: $node_count"
}

# List clusters
list_clusters() {
    log_info "Configured clusters:"

    if [ ! -d "$CLUSTERS_CONFIG_DIR" ]; then
        log_info "No clusters configured"
        return 0
    fi

    for cluster_dir in "$CLUSTERS_CONFIG_DIR"/*; do
        if [ -d "$cluster_dir" ]; then
            local cluster_name=$(basename "$cluster_dir")
            local elasticsearch_config="${cluster_dir}/elasticsearch.yml"

            if [ -f "$elasticsearch_config" ]; then
                # Count nodes by checking node config files
                local node_count=0
                for node_config in "${cluster_dir}"/*.yml; do
                    if [[ "$(basename "$node_config")" =~ ^${cluster_name}-node[0-9]+\.yml$ ]]; then
                        node_count=$((node_count + 1))
                    fi
                done

                # Check if running
                local running_nodes=0
                local pid_file="${ES_BASE_DIR}/pids/${cluster_name}.pids"
                if [ -f "$pid_file" ]; then
                    while IFS=':' read -r pid node_name http_port; do
                        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                            running_nodes=$((running_nodes + 1))
                        fi
                    done < "$pid_file"
                fi

                printf "  %-20s Nodes: %-2d/%d Status: %s\\n" \
                    "$cluster_name" "$running_nodes" "$node_count" \
                    "$([ "$running_nodes" -gt 0 ] && echo "Running" || echo "Stopped")"
            fi
        fi
    done
}

# Add node to existing cluster
add_node() {
    local cluster_name="$1"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    # Count existing nodes
    local current_node_count=0
    for node_config in "${cluster_dir}"/*.yml; do
        if [[ "$(basename "$node_config")" =~ ^${cluster_name}-node[0-9]+\.yml$ ]]; then
            current_node_count=$((current_node_count + 1))
        fi
    done

    local new_node_count=$((current_node_count + 1))

    log_info "Adding node to cluster '$cluster_name'..."

    # Get base ports from the first node config
    local base_http_port="$DEFAULT_BASE_HTTP_PORT"
    local base_transport_port="$DEFAULT_BASE_TRANSPORT_PORT"

    if [ $current_node_count -gt 0 ]; then
        local first_node_name=$(get_node_name "$cluster_name" "1")
        base_http_port=$(get_node_config "$cluster_name" "$first_node_name" "http.port" "$DEFAULT_BASE_HTTP_PORT")
        base_transport_port=$(get_node_config "$cluster_name" "$first_node_name" "transport.port" "$DEFAULT_BASE_TRANSPORT_PORT")

        # Adjust base ports to actual base values (node 1 port)
        base_http_port=$((base_http_port - 0))  # Node 1 has index 1, so subtract (1-1)=0
        base_transport_port=$((base_transport_port - 0))
    fi

    # Generate new node configuration
    local ports=$(calculate_ports "$cluster_name" "$new_node_count" "$base_http_port" "$base_transport_port" "$DEFAULT_BASE_JVM_HEAP")
    local http_port=$(echo "$ports" | cut -d':' -f1)
    local transport_port=$(echo "$ports" | cut -d':' -f2)
    local debug_port=$(echo "$ports" | cut -d':' -f3)

    # Get JVM heap setting from an existing node config
    local jvm_heap="$DEFAULT_JVM_HEAP"
    local sample_node_config=$(ls "${cluster_dir}"/*.yml | grep -E "${cluster_name}-node[0-9]+\.yml" | head -1)
    if [ -n "$sample_node_config" ]; then
        jvm_heap=$(grep "^-Xmx" "${sample_node_config%.yml}.jvm.options" | sed 's/-Xmx//' | head -1)
    fi

    generate_node_config "$cluster_name" "$new_node_count" "$new_node_count" "$http_port" "$transport_port" "$jvm_heap" "$cluster_dir" "$debug_port"

    log_success "Added node $(get_node_name "$cluster_name" "$new_node_count") to cluster '$cluster_name'"
    log_info "New node count: $new_node_count"
}

# Remove node from cluster
remove_node() {
    local cluster_name="$1"
    local node_name="$2"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    # Stop the node if running
    local node_index=$(echo "$node_name" | sed 's/.*-node//')
    if [ -z "$node_index" ] || ! [[ "$node_index" =~ ^[0-9]+$ ]]; then
        log_error "Invalid node name format: $node_name"
        return 1
    fi

    log_info "Removing node $node_name from cluster '$cluster_name'..."

    # Stop the node
    stop_cluster "$cluster_name" "$node_index"

    # Clean up data and logs
    rm -rf "${ES_BASE_DIR}/data/${cluster_name}/${node_name}"
    rm -rf "${ES_BASE_DIR}/logs/${cluster_name}/${node_name}"
    rm -f "${cluster_dir}/${node_name}.yml"
    rm -f "${cluster_dir}/${node_name}.jvm.options"

    log_success "Removed node $node_name from cluster '$cluster_name'"
}

# Cleanup cluster
cleanup_cluster() {
    local cluster_name="$1"

    if [ "${FORCE:-false}" != "true" ]; then
        log_warning "This will delete all data and logs for cluster '$cluster_name'"
        read -p "Are you sure? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Cleanup cancelled"
            return 0
        fi
    fi

    # Stop cluster if running
    stop_cluster "$cluster_name" true

    # Remove data and logs
#    rm -rf "${ES_BASE_DIR}/config/clusters/${cluster_name}"
#    rm -rf "${ES_BASE_DIR}/data/${cluster_name}" # NOTE DO NOT REMOVE DATA
    rm -rf "${ES_BASE_DIR}/logs/${cluster_name}"
    rm -f "${ES_BASE_DIR}/pids/${cluster_name}.pids"

    log_success "Cleaned up cluster '$cluster_name'"
}

# Show logs
show_logs() {
    local cluster_name="$1"
    local node_name="${2:-}"

    if [ -z "$node_name" ]; then
        # Show all logs
        local logs_dir="${ES_BASE_DIR}/logs/${cluster_name}"
        if [ -d "$logs_dir" ]; then
            log_info "Available log files:"
            find "$logs_dir" -name "*.log" -type f | sort
        else
            log_error "No logs found for cluster '$cluster_name'"
        fi
    else
        # Show specific node logs
        local log_file="${ES_BASE_DIR}/logs/${cluster_name}/${node_name}/stdout.log"
        if [ -f "$log_file" ]; then
            tail -f "$log_file"
        else
            log_error "Log file not found: $log_file"
        fi
    fi
}

# Show configuration
show_config() {
    local cluster_name="$1"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    log_info "=== Configuration for cluster: $cluster_name ==="
    if [ -f "${cluster_dir}/elasticsearch.yml" ]; then
        log_info "--- Common configuration (elasticsearch.yml) ---"
        cat "${cluster_dir}/elasticsearch.yml"
    else
        log_warning "Cluster configuration file not found"
    fi

    log_info ""
    log_info "=== Node configurations ==="
    for node_file in "${cluster_dir}"/*-node*.yml; do
        if [ -f "$node_file" ]; then
            log_info "--- Node: $(basename "$node_file" .yml) ---"
            cat "$node_file"

            log_info "--- JVM options for $(basename "$node_file" .yml) ---"
            local jvm_options_file="${node_file%.yml}.jvm.options"
            if [ -f "$jvm_options_file" ]; then
                cat "$jvm_options_file"
            else
                log_warning "JVM options file not found: $jvm_options_file"
            fi
        fi
    done
}

# Setup security certificates
setup_security() {
    local cluster_name="$1"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    log_info "Setting up SSL certificates for cluster '$cluster_name'..."

    # Create certs directory
    local certs_dir="${cluster_dir}/certs"
    mkdir -p "$certs_dir"

    # Generate CA certificate
    log_info "Generating CA certificate..."
    "${ES_INSTALL_DIR}/bin/elasticsearch-certutil" ca \
        --out "${certs_dir}/elastic-stack-ca.p12" \
        --pass "" \
        --silent

    if [ $? -ne 0 ]; then
        log_error "Failed to generate CA certificate"
        return 1
    fi

    # Generate node certificates
    log_info "Generating node certificates..."
    "${ES_INSTALL_DIR}/bin/elasticsearch-certutil" cert \
        --ca "${certs_dir}/elastic-stack-ca.p12" \
        --ca-pass "" \
        --out "${certs_dir}/elastic-certificates.p12" \
        --pass "" \
        --silent

    if [ $? -ne 0 ]; then
        log_error "Failed to generate node certificates"
        return 1
    fi

    log_success "Certificates generated successfully"

    # Create required user files
    log_info "Creating required user configuration files..."
    touch "${cluster_dir}/users"
    touch "${cluster_dir}/users_roles"

    # Update elasticsearch.yml to include certificate configuration
    local es_config="${cluster_dir}/elasticsearch.yml"
    if ! grep -q "xpack.security.transport.ssl.keystore.path" "$es_config"; then
        log_info "Updating elasticsearch.yml with certificate configuration..."
        cat >> "$es_config" << EOF

# SSL Certificate Configuration
xpack.security.transport.ssl.keystore.path: certs/elastic-certificates.p12
xpack.security.transport.ssl.truststore.path: certs/elastic-certificates.p12
xpack.security.transport.ssl.verification_mode: certificate
EOF
        log_success "Configuration updated"
    else
        log_info "Certificate configuration already exists in elasticsearch.yml"
    fi

    log_success "Security setup completed for cluster '$cluster_name'"
    log_info "Certificate files:"
    log_info "  CA: ${certs_dir}/elastic-stack-ca.p12"
    log_info "  Node cert: ${certs_dir}/elastic-certificates.p12"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Start the cluster: $0 start $cluster_name"
    log_info "  2. Set password: $0 set-password $cluster_name"
}

# Set password for elastic user
set_password() {
    local cluster_name="$1"
    local password="${2:-}"

    local cluster_dir="${CLUSTERS_CONFIG_DIR}/${cluster_name}"
    if [ ! -d "$cluster_dir" ]; then
        log_error "Cluster '$cluster_name' not found"
        return 1
    fi

    # Validate password length if provided
    if [ -n "$password" ] && [ ${#password} -lt 6 ]; then
        log_error "Password must be at least 6 characters long"
        log_info "Provided password length: ${#password}"
        return 1
    fi

    # Check if cluster is running
    local pid_file="${ES_BASE_DIR}/pids/${cluster_name}.pids"
    if [ ! -f "$pid_file" ]; then
        log_error "Cluster '$cluster_name' is not running. Please start it first."
        return 1
    fi

    # Get HTTP port from first running node
    local http_port=""
    while IFS=':' read -r pid node_name port; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            http_port="$port"
            break
        fi
    done < "$pid_file"

    if [ -z "$http_port" ]; then
        log_error "No running nodes found for cluster '$cluster_name'"
        return 1
    fi

    log_info "Setting password for elastic user..."
    log_info "Waiting for cluster to be ready..."

    # Wait for cluster to be ready (up to 60 seconds)
    local ready=false
    for i in {1..60}; do
        if curl -s "http://localhost:${http_port}/_cluster/health" > /dev/null 2>&1; then
            ready=true
            log_success "Cluster is ready"
            break
        fi
        sleep 1
        if [ $((i % 10)) -eq 0 ]; then
            log_info "Still waiting... ($i/60)"
        fi
    done

    if [ "$ready" = false ]; then
        log_error "Cluster is not ready after 60 seconds. Please check cluster status."
        return 1
    fi

    # Use elasticsearch-reset-password tool
    log_info "Using elasticsearch-reset-password tool..."

    if [ ! -x "${ES_INSTALL_DIR}/bin/elasticsearch-reset-password" ]; then
        log_error "elasticsearch-reset-password tool not found"
        return 1
    fi

    # Set environment
    export ES_PATH_CONF="${cluster_dir}"

    local output
    local exit_code
    local final_password

    if [ -n "$password" ]; then
        # Use specified password - use two-step approach
        log_info "Setting custom password for user 'elastic'..."

        # Step 1: Generate a temporary random password
        log_info "Step 1: Generating temporary password..."
        output=$(ES_PATH_CONF="${cluster_dir}" \
            "${ES_INSTALL_DIR}/bin/elasticsearch-reset-password" \
            -u elastic \
            -b \
            -a \
            --url "http://localhost:${http_port}" 2>&1)
        exit_code=$?

        # Extract the generated password from output
        local temp_password=$(echo "$output" | grep -oP 'New value: \K.*' | head -1)

        if [ $exit_code -ne 0 ] || [ -z "$temp_password" ]; then
            log_error "Failed to generate temporary password"
            log_info "Output: $output"
            return 1
        fi

        log_info "Temporary password generated successfully"

        # Step 2: Wait a moment for the password to be applied
        sleep 2

        # Step 3: Change to the desired password using API
        log_info "Step 2: Changing to custom password..."
        local result=$(curl -s -w "\n%{http_code}" -X POST \
            -u "elastic:$temp_password" \
            "http://localhost:${http_port}/_security/user/elastic/_password" \
            -H "Content-Type: application/json" \
            -d "{\"password\":\"$password\"}" 2>&1)

        local http_code=$(echo "$result" | tail -n1)
        local response_body=$(echo "$result" | head -n-1)

        if [ "$http_code" = "200" ]; then
            log_success "Password changed to custom value successfully"
            final_password="$password"
        else
            log_error "Failed to change password via API (HTTP $http_code)"
            log_info "Response: $response_body"
            log_info "Temporary password was: $temp_password"
            log_warning "You can try manually: curl -u elastic:$temp_password -X POST http://localhost:${http_port}/_security/user/elastic/_password -H 'Content-Type: application/json' -d '{\"password\":\"YOUR_PASSWORD\"}'"
            return 1
        fi
    else
        # Use auto mode to generate password
        log_info "Generating random password for user 'elastic'..."
        output=$(ES_PATH_CONF="${cluster_dir}" \
            "${ES_INSTALL_DIR}/bin/elasticsearch-reset-password" \
            -u elastic \
            -b \
            -a \
            --url "http://localhost:${http_port}" 2>&1)
        exit_code=$?
        # Extract the generated password from output
        final_password=$(echo "$output" | grep -oP 'New value: \K.*' | head -1)

        if [ $exit_code -ne 0 ] || [ -z "$final_password" ]; then
            log_error "Failed to generate password"
            log_info "Output: $output"
            return 1
        fi
    fi

    log_success "Password set successfully for user 'elastic'"
    log_info "Username: elastic"
    log_info "Password: ${final_password}"
    log_info ""
    if [ -z "$password" ]; then
        log_info "IMPORTANT: Save this password! It cannot be recovered."
        log_info ""
    fi
    log_info "Test connection:"
    log_info "  curl -u elastic:${final_password} http://localhost:${http_port}"
    log_info "  curl -u elastic:${final_password} http://localhost:${http_port}/_cluster/health"
}

# Main execution
main() {
    check_dependencies

    if [ $# -eq 0 ]; then
        help
        exit 1
    fi

    local command="$1"
    shift

    # Parse global options
    while [[ $# -gt 0 ]]; do
        case $1 in
            -f|--force)
                FORCE="true"
                shift
                ;;
            -v|--verbose)
                VERBOSE="true"
                shift
                ;;
            -h|--help)
                help
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done

    case "$command" in
        init)
            local cluster_name=""
            local node_count="$DEFAULT_NODE_COUNT"
            local template="dev"
            local jvm_heap="$DEFAULT_JVM_HEAP"
            local base_http_port="$DEFAULT_BASE_HTTP_PORT"
            local enable_security="false"

            while [[ $# -gt 0 ]]; do
                case $1 in
                    -n|--nodes)
                        node_count="$2"
                        shift 2
                        ;;
                    -t|--template)
                        template="$2"
                        shift 2
                        ;;
                    -j|--jvm-heap)
                        jvm_heap="$2"
                        shift 2
                        ;;
                    -p|--http-port)
                        base_http_port="$2"
                        shift 2
                        ;;
                    -s|--security)
                        enable_security="true"
                        shift
                        ;;
                    -f|--force)
                        FORCE="true"
                        shift
                        ;;
                    -h|--help)
                        help
                        exit 0
                        ;;
                    *)
                        if [ -z "$cluster_name" ]; then
                            cluster_name="$1"
                        else
                            log_error "Unknown option: $1"
                            exit 1
                        fi
                        shift
                        ;;
                esac
            done

            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi

            init_cluster "$cluster_name" "$node_count" "$template" "$jvm_heap" "$base_http_port" "$enable_security"
            ;;

        start)
            local cluster_name="$1"
            local specific_nodes=""

            shift
            while [[ $# -gt 0 ]]; do
                case $1 in
                    --nodes)
                        specific_nodes="$2"
                        shift 2
                        ;;
                    *)
                        break
                        ;;
                esac
            done

            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi

            start_cluster "$cluster_name" "$specific_nodes"
            ;;

        stop)
            local cluster_name="$1"
            local specific_nodes=""
            local force=false

            shift
            while [[ $# -gt 0 ]]; do
                case $1 in
                    -f|--force)
                        force="true"
                        shift
                        ;;
                    --nodes)
                        specific_nodes="$2"
                        shift 2
                        ;;
                    *)
                        break
                        ;;
                esac
            done

            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi

            stop_cluster "$cluster_name" "$specific_nodes" "$force"
            ;;

        restart)
            local cluster_name="$1"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            stop_cluster "$cluster_name" "" true
            sleep 2
            start_cluster "$cluster_name"
            ;;

        status)
            local cluster_name="$1"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            status_cluster "$cluster_name"
            ;;

        list)
            list_clusters
            ;;

        add-node)
            local cluster_name="$1"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            add_node "$cluster_name"
            ;;

        remove-node)
            local cluster_name="$1"
            local node_name="$2"
            if [ -z "$cluster_name" ] || [ -z "$node_name" ]; then
                log_error "Cluster name and node name are required"
                exit 1
            fi
            remove_node "$cluster_name" "$node_name"
            ;;

        cleanup)
            local cluster_name="$1"
            local force=false

            shift
            while [[ $# -gt 0 ]]; do
                case $1 in
                    -f|--force)
                        force="true"
                        shift
                        ;;
                    *)
                        break
                        ;;
                esac
            done

            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi

            FORCE="$force" cleanup_cluster "$cluster_name"
            ;;

        logs)
            local cluster_name="$1"
            local node_name="$2"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            show_logs "$cluster_name" "$node_name"
            ;;

        config)
            local cluster_name="$1"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            show_config "$cluster_name"
            ;;

        setup-security)
            local cluster_name="$1"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            setup_security "$cluster_name"
            ;;

        set-password)
            local cluster_name="$1"
            local password="${2:-elastic}"
            if [ -z "$cluster_name" ]; then
                log_error "Cluster name is required"
                exit 1
            fi
            set_password "$cluster_name" "$password"
            ;;

        *)
            log_error "Unknown command: $command"
            help
            exit 1
            ;;
    esac
}

main "$@"
