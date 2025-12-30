# Enhanced Elasticsearch Cluster Manager Documentation

## Overview

The Enhanced Elasticsearch Cluster Manager (`es-cluster-manager.sh`) provides a comprehensive solution for managing single and multi-cluster Elasticsearch deployments. It uses a configuration-driven approach with YAML-based setup and supports easy cluster/node addition.

## Quick Start

### 1. Initialize a New Cluster

```bash
# Basic 3-node cluster
./es-cluster-manager.sh init my-cluster

# Custom configuration
./es-cluster-manager.sh init my-cluster -n 5 -t production -j 2g -p 9500

# With security enabled
./es-cluster-manager.sh init secure-cluster -n 3 -s
```

### 2. Start Cluster

```bash
# Start all nodes
./es-cluster-manager.sh start my-cluster

# Start specific nodes
./es-cluster-manager.sh start my-cluster --nodes 1,3

# Start single node
./es-cluster-manager.sh start my-cluster --nodes 2
```

### 3. Check Status

```bash
./es-cluster-manager.sh status my-cluster
```

### 4. Stop Cluster

```bash
# Stop all nodes gracefully
./es-cluster-manager.sh stop my-cluster

# Stop specific nodes
./es-cluster-manager.sh stop my-cluster --nodes 1,2

# Force stop all nodes
./es-cluster-manager.sh stop my-cluster --force
```

### 5. List All Clusters

```bash
./es-cluster-manager.sh list
```

## Configuration Structure

### Directory Layout
```
config/
├── clusters/
│   └── <cluster-name>/
│       ├── cluster.yml          # Cluster-wide configuration
│       ├── jvm.options          # JVM settings for all nodes
│       ├── log4j2.properties    # Logging configuration
│       ├── <node-name>.yml      # Individual node configuration
│       └── elasticsearch.yml    # Base configuration
```

### Cluster Configuration (cluster.yml)
```yaml
cluster.name: my-cluster
cluster.node_count: 3
cluster.template: dev
cluster.jvm_heap: 1g
cluster.http_port: 9200
cluster.transport_port: 9300
cluster.security_enabled: false
cluster.created_at: 2024-01-01T00:00:00Z
```

### Node Configuration Pattern
- **Node Name**: `{cluster-name}-node{index}`
- **HTTP Port**: `base_http_port + (cluster_offset * 10) + (node_index - 1)`
- **Transport Port**: `base_transport_port + (cluster_offset * 10) + (node_index - 1)`

## Advanced Usage

### Adding Nodes to Existing Cluster

```bash
# Add one node
./es-cluster-manager.sh add-node my-cluster

# Add multiple nodes (run multiple times)
for i in {4..6}; do
    ./es-cluster-manager.sh add-node my-cluster
done
```

### Custom JVM Configuration

Edit `config/clusters/<cluster-name>/jvm.options`:

```bash
# Basic heap settings
-Xms2g
-Xmx2g

# G1GC tuning
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:+DisableExplicitGC

# GC logging
-Xlog:gc*,gc+age=trace,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m
```

### Multi-Cluster Setup

```bash
# Cluster 1: Development
./es-cluster-manager.sh init dev-cluster -n 1 -t dev -j 512m -p 9200

# Cluster 2: Testing
./es-cluster-manager.sh init test-cluster -n 3 -t test -j 1g -p 9400

# Cluster 3: Production-like
./es-cluster-manager.sh init prod-cluster -n 5 -t production -j 2g -p 9600

# Start all clusters
./es-cluster-manager.sh start dev-cluster
./es-cluster-manager.sh start test-cluster
./es-cluster-manager.sh start prod-cluster
```

## Configuration Templates

### Basic Template
- Single-node cluster
- Minimal configuration
- Development use

### Dev Template
- Multi-node cluster
- Development settings
- CORS enabled
- Security disabled

### Test Template
- Multi-node cluster
- Testing configuration
- Moderate resource usage

### Production Template
- Multi-node cluster
- Production settings
- Security enabled
- Optimized JVM settings

## Port Allocation Strategy

### Automatic Port Assignment
Each cluster gets a unique port range based on its name hash:

```
Cluster: my-cluster
- Node 1: HTTP 9200, Transport 9300
- Node 2: HTTP 9201, Transport 9301
- Node 3: HTTP 9202, Transport 9302

Cluster: another-cluster
- Node 1: HTTP 9210, Transport 9310  (offset +10)
- Node 2: HTTP 9211, Transport 9311
- Node 3: HTTP 9212, Transport 9312
```

### Manual Port Configuration
Override in cluster.yml:
```yaml
cluster.http_port: 9500
cluster.transport_port: 9600
```

## Monitoring and Debugging

### View Logs
```bash
# All logs for cluster
./es-cluster-manager.sh logs my-cluster

# Specific node logs
./es-cluster-manager.sh logs my-cluster my-cluster-node1
```

### Check Configuration
```bash
./es-cluster-manager.sh config my-cluster
```

### Debug Mode
```bash
VERBOSE=true ./es-cluster-manager.sh start my-cluster
```

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   - Check with: `lsof -i :9200`
   - Solution: Use different base port or stop conflicting service

2. **Node Won't Start**
   - Check logs: `./es-cluster-manager.sh logs my-cluster`
   - Verify disk space and permissions
   - Check JVM heap size

3. **Cluster Not Forming**
   - Verify all nodes use same cluster.name
   - Check discovery.seed_hosts configuration
   - Ensure network connectivity

### Debug Commands

```bash
# Check running processes
ps aux | grep elasticsearch

# Check ports
netstat -tlnp | grep :920

# Check cluster health
curl http://localhost:9200/_cluster/health?pretty

# Check node info
curl http://localhost:9200/_nodes?pretty
```

## Best Practices

### 1. Naming Conventions
- Use descriptive cluster names: `dev-search`, `prod-analytics`
- Follow pattern: `{environment}-{purpose}`

### 2. Resource Planning
- **Development**: 512MB-1GB heap, 1-2 nodes
- **Testing**: 1-2GB heap, 3-5 nodes
- **Production**: 2-8GB heap, 3+ nodes

### 3. JVM Settings
- Set heap to 50% of available RAM (max 32GB)
- Use G1GC for heaps > 4GB
- Enable GC logging for production

### 4. Security
- Enable security for production clusters
- Use certificates for transport layer
- Configure proper user authentication

### 5. Backup Strategy
- Regular snapshots for production data
- Test restore procedures
- Document cluster configurations

## Migration from Old Scripts

### From es-manager.sh
1. Initialize new clusters with enhanced manager
2. Copy custom configurations
3. Update automation scripts

```bash
# Old way
./es-manager.sh setup my-cluster -n 3

# New way
./es-cluster-manager.sh init my-cluster -n 3
```

## Environment Variables

```bash
# Set custom Java home
export ES_JAVA_HOME=/opt/java/openjdk-17

# Enable debug mode
export VERBOSE=true

# Force operations
export FORCE=true
```

## Integration Examples

### Docker Integration
```dockerfile
FROM elasticsearch:8.16.1
COPY config/clusters /usr/share/elasticsearch/config/clusters
```

### CI/CD Pipeline
```yaml
# GitHub Actions example
- name: Setup Elasticsearch
  run: |
    ./dev-utils/es-cluster-manager.sh init test-cluster -n 1
    ./dev-utils/es-cluster-manager.sh start test-cluster
    ./dev-utils/es-cluster-manager.sh status test-cluster
```

### Shell Script Integration
```bash
#!/bin/bash
# test-setup.sh

CLUSTER_NAME="test-$(date +%s)"
./es-cluster-manager.sh init "$CLUSTER_NAME" -n 1 -t test
./es-cluster-manager.sh start "$CLUSTER_NAME"

# Wait for cluster
sleep 10

# Run tests
npm test -- --elasticsearch-url=http://localhost:9200

# Cleanup
./es-cluster-manager.sh cleanup "$CLUSTER_NAME"
```
