## Running Elasticsearch in 2-Node Cluster with Docker Compose

This guide explains how to set up and run the 2-node Elasticsearch cluster using the provided `docker-compose.yml` file.

### Prerequisites

- Docker installed and running
- Docker Compose installed
- At least 2GB of available RAM (1GB per Elasticsearch node)
- Network connectivity between nodes if running on separate machines

### Configuration

The `docker-compose.yml` file is configured for a 2-node Elasticsearch cluster. To set up the cluster properly, you need to make the following changes before running:

1. Update the `discovery.seed_hosts` with the IP addresses of both nodes:
   - Replace `[IP_NODE_1]` with the IP address of the first node
   - Replace `[IP_NODE_2]` with the IP address of the second node

2. Update the `network.publish_host` with the IP address of the current node:
   - For the first node, set to the actual IP of that machine
   - For the second node, you'll need to create a separate docker-compose file or use environment variables

### Running the Cluster

#### Method 1: Single Machine with Multiple Services

To simulate a 2-node cluster on a single machine, update the docker-compose.yml file to include both services:

```yaml
services:
  es01:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.10.4
    container_name: es01
    environment:
      - node.name=es01
      - cluster.name=es-docker-cluster
      - discovery.seed_hosts=es02
      - cluster.initial_master_nodes=es01,es02
      - network.publish_host=es01
      - ES_JAVA_OPTS=-Xms512m -Xmx512m
      - xpack.security.enabled=false
    volumes:
      - esdata01:/usr/share/elasticsearch/data
    ports:
      - '9200:9200'
      - '9300:9300'
    networks:
      - elastic
    ulimits:
      memlock:
        soft: -1
        hard: -1

  es02:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.10.4
    container_name: es02
    environment:
      - node.name=es02
      - cluster.name=es-docker-cluster
      - discovery.seed_hosts=es01
      - cluster.initial_master_nodes=es01,es02
      - network.publish_host=es02
      - ES_JAVA_OPTS=-Xms512m -Xmx512m
      - xpack.security.enabled=false
    volumes:
      - esdata02:/usr/share/elasticsearch/data
    ports:
      - '9201:9200'
      - '9301:9300'
    networks:
      - elastic
    ulimits:
      memlock:
        soft: -1
        hard: -1

volumes:
  esdata01:
    driver: local
  esdata02:
    driver: local

networks:
  elastic:
    driver: bridge
```

Then run:
```bash
docker-compose up -d
```

#### Method 2: Separate Machines (True Distributed Setup)

1. On Node 1 (IP: [IP_NODE_1]):
   - Update the docker-compose.yml with:
     ```yaml
     environment:
       - node.name=es01
       - cluster.name=es-docker-cluster
       - discovery.seed_hosts=[IP_NODE_1],[IP_NODE_2]
       - cluster.initial_master_nodes=es01,es02
       - network.publish_host=[IP_NODE_1]
       - ES_JAVA_OPTS=-Xms512m -Xmx512m
       - xpack.security.enabled=false
     ```
   - Run: `docker-compose up -d`

2. On Node 2 (IP: [IP_NODE_2]):
   - Update the docker-compose.yml with:
     ```yaml
     environment:
       - node.name=es02
       - cluster.name=es-docker-cluster
       - discovery.seed_hosts=[IP_NODE_1],[IP_NODE_2]
       - cluster.initial_master_nodes=es01,es02
       - network.publish_host=[IP_NODE_2]
       - ES_JAVA_OPTS=-Xms512m -Xmx512m
       - xpack.security.enabled=false
     ```
   - Run: `docker-compose up -d`

### Verifying the Cluster

After starting the cluster, verify that both nodes are running and connected:

```bash
curl -X GET "localhost:9200/_cluster/health?pretty"
```

For the 2-node setup on a single machine, check the second node:
```bash
curl -X GET "localhost:9201/_cluster/health?pretty"
```

To see cluster information:
```bash
curl -X GET "localhost:9200/_cat/nodes?v&pretty"
```

### Useful Elasticsearch Commands

#### Basic Cluster Information
```bash
# Check cluster health
curl -X GET "localhost:9200/_cluster/health?pretty"

# List nodes in the cluster
curl -X GET "localhost:9200/_cat/nodes?v&h=ip,port,node.role,name"

# Check cluster settings
curl -X GET "localhost:9200/_cluster/settings?pretty"
```

#### Index Management
```bash
# List all indices
curl -X GET "localhost:9200/_cat/indices?v&pretty"

# Create an index
curl -X PUT "localhost:9200/my-index?pretty" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 1
  }
}
'

# Delete an index
curl -X DELETE "localhost:9200/my-index?pretty"
```

#### Document Operations
```bash
# Index a document
curl -X PUT "localhost:9200/my-index/_doc/1?pretty" -H 'Content-Type: application/json' -d'
{
  "name": "Test Document",
  "description": "A sample document for testing"
}
'

# Get a document
curl -X GET "localhost:9200/my-index/_doc/1?pretty"

# Search documents
curl -X GET "localhost:9200/my-index/_search?q=name:test&pretty"
```

#### Cluster Monitoring
```bash
# Get cluster statistics
curl -X GET "localhost:9200/_cluster/stats?pretty"

# Get node statistics
curl -X GET "localhost:9200/_nodes/stats?pretty"

# Get information about ongoing tasks
curl -X GET "localhost:9200/_tasks?detailed=true&actions=*&pretty"
```

### Stopping the Cluster

To stop and remove the containers:
```bash
docker-compose down
```

To stop and remove the containers and volumes (will delete all data):
```bash
docker-compose down -v
```

### Troubleshooting

- **Cluster not forming**: Ensure that both nodes can communicate on port 9300 and that the IP addresses in `discovery.seed_hosts` are accessible from both nodes.
- **Insufficient memory**: Increase the heap size by modifying `ES_JAVA_OPTS` (minimum recommended is 1GB: `-Xms1g -Xmx1g`).
- **Permission issues**: Ensure the Elasticsearch user has write permissions to the volume directories.
- **Port conflicts**: If ports 9200/9300 are already in use, change the port mappings in the docker-compose.yml file.