#!/bin/bash

# Quick test script for Enhanced Elasticsearch Cluster Manager
# Tests basic functionality with a single-node cluster

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_MANAGER="${SCRIPT_DIR}/es-cluster-manager.sh"

echo "=== Enhanced Elasticsearch Cluster Manager Quick Test ==="
echo

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Test cluster name
TEST_CLUSTER="quick-test-$$"

# Cleanup function
cleanup() {
    echo
    echo "Cleaning up test cluster..."
    "$CLUSTER_MANAGER" stop "$TEST_CLUSTER" --force 2>/dev/null || true
    "$CLUSTER_MANAGER" cleanup "$TEST_CLUSTER" --force 2>/dev/null || true
}

# Set trap for cleanup
trap cleanup EXIT

# 1. Initialize cluster
echo "1. Initializing test cluster..."
if "$CLUSTER_MANAGER" init "$TEST_CLUSTER" -n 1 -t basic -j 512m; then
    echo -e "${GREEN}✓ Cluster initialized successfully${NC}"
else
    echo -e "${RED}✗ Failed to initialize cluster${NC}"
    exit 1
fi

# 2. List clusters
echo
echo "2. Listing clusters..."
"$CLUSTER_MANAGER" list

# 3. Start cluster
echo
echo "3. Starting cluster..."
if "$CLUSTER_MANAGER" start "$TEST_CLUSTER"; then
    echo -e "${GREEN}✓ Cluster started successfully${NC}"
else
    echo -e "${RED}✗ Failed to start cluster${NC}"
    exit 1
fi

# 4. Check status
echo
echo "4. Checking cluster status..."
"$CLUSTER_MANAGER" status "$TEST_CLUSTER"

# 5. Test basic connectivity
echo
echo "5. Testing connectivity..."
sleep 10

# Get the HTTP port from configuration
HTTP_PORT=$(grep "http.port:" "/Users/cdj/code/work/elasticsearch/build/distribution/local/elasticsearch-8.16.1-SNAPSHOT/config/clusters/$TEST_CLUSTER/$TEST_CLUSTER-node1.yml" | cut -d':' -f2- | tr -d ' ')

if curl -s "http://localhost:$HTTP_PORT/_cluster/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Cluster is responding on port $HTTP_PORT${NC}"
    echo "   Health check:"
    curl -s "http://localhost:$HTTP_PORT/_cluster/health?pretty" 2>/dev/null || echo "   (Requires authentication)"
elif curl -s "http://localhost:$HTTP_PORT/" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Cluster is responding on port $HTTP_PORT${NC}"
    echo -e "${YELLOW}⚠ Cluster requires authentication${NC}"
    echo "   Try: curl -u elastic:password http://localhost:$HTTP_PORT/_cluster/health"
else
    echo -e "${YELLOW}⚠ Cluster may still be starting or require authentication${NC}"
    echo "   HTTP port: $HTTP_PORT"
fi

# 6. Show configuration
echo
echo "6. Cluster configuration:"
"$CLUSTER_MANAGER" config "$TEST_CLUSTER"

# 7. Test multi-node cluster
echo
echo "7. Testing multi-node cluster..."
MULTI_CLUSTER="multi-test-$$"
"$CLUSTER_MANAGER" init "$MULTI_CLUSTER" -n 2 -t dev -j 512m
"$CLUSTER_MANAGER" start "$MULTI_CLUSTER"
sleep 5
"$CLUSTER_MANAGER" status "$MULTI_CLUSTER"
"$CLUSTER_MANAGER" stop "$MULTI_CLUSTER" --force
"$CLUSTER_MANAGER" cleanup "$MULTI_CLUSTER" --force

echo
echo -e "${GREEN}=== Quick test completed successfully! ===${NC}"
echo
echo "Usage examples:"
echo "  ./es-cluster-manager.sh init my-cluster -n 3 -t dev -j 1g"
echo "  ./es-cluster-manager.sh start my-cluster"
echo "  ./es-cluster-manager.sh status my-cluster"
echo "  ./es-cluster-manager.sh add-node my-cluster"
echo "  ./es-cluster-manager.sh stop my-cluster"
echo "  ./es-cluster-manager.sh list"
echo
echo "Documentation: ./dev-utils/README-CLUSTER-MANAGER.md"