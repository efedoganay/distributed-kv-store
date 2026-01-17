# Distributed Key-Value Store

A fault-tolerant distributed key-value store implementation using **Apache Thrift** for RPC communication and **Apache ZooKeeper** (via Curator) for coordination and leader election.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ZooKeeper Cluster                        │
│                    (Service Discovery & Leader Election)        │
└─────────────────────────────────────────────────────────────────┘
                    ▲                           ▲
                    │ watch/register            │ watch/register
                    │                           │
┌───────────────────┴───────────────┐ ┌────────┴───────────────────┐
│         StorageNode (Primary)     │ │    StorageNode (Backup)    │
│  ┌─────────────────────────────┐  │ │  ┌─────────────────────────┐│
│  │     KeyValueHandler         │  │ │  │     KeyValueHandler     ││
│  │  ┌───────────────────────┐  │  │ │  │  ┌───────────────────┐  ││
│  │  │  ConcurrentHashMap    │  │◄─┼─┼──┤  │  ConcurrentHashMap │  ││
│  │  │   (Key-Value Store)   │  │  │ │  │  │   (Replicated)     │  ││
│  │  └───────────────────────┘  │  │ │  │  └───────────────────┘  ││
│  └─────────────────────────────┘  │ │  └─────────────────────────┘│
└───────────────────────────────────┘ └────────────────────────────┘
                    ▲
                    │ Thrift RPC (get/put)
                    │
          ┌─────────┴─────────┐
          │      Client       │
          │  (Multi-threaded) │
          └───────────────────┘
```

## Features

- **Primary-Backup Replication**: Automatic failover with data synchronization
- **Leader Election**: ZooKeeper-based leader election using ephemeral sequential nodes
- **Striped Locking**: Fine-grained concurrency control with Guava's `Striped<Lock>`
- **Automatic Recovery**: Backup nodes sync state from primary on startup
- **High-Throughput Client**: Multi-threaded client with connection pooling

## Components

| Component | Description |
|-----------|-------------|
| `StorageNode.java` | Server implementation with ZooKeeper integration |
| `KeyValueHandler.java` | Thrift service handler with replication logic |
| `Client.java` | Multi-threaded test client with throughput measurement |
| `CreateZNode.java` | Utility to create ZooKeeper parent node |
| `a3.thrift` | Thrift service definition |

## Prerequisites

- Java 11+
- Apache Thrift
- Apache ZooKeeper (running cluster)
- Apache Curator
- Guava

## Building

```bash
# Generate Thrift code
thrift -r --gen java a3.thrift

# Compile (adjust classpath as needed)
javac -cp ".:lib/*:gen-java" *.java
```

## Usage

### 1. Create ZooKeeper Node
```bash
java CreateZNode localhost:2181 /kvstore
```

### 2. Start Storage Nodes
```bash
# Primary (first node to register becomes primary)
java StorageNode localhost 9090 localhost:2181 /kvstore

# Backup (additional nodes become backups)
java StorageNode localhost 9091 localhost:2181 /kvstore
```

### 3. Run Client
```bash
java Client localhost:2181 /kvstore <num_threads> <duration_seconds> <keyspace_size>

# Example: 4 threads, 10 seconds, 1000 keys
java Client localhost:2181 /kvstore 4 10 1000
```

## API

```thrift
service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void replicate(1: string key, 2: string value);
  map<string, string> dump();
}
```

## License

MIT License
