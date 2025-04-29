# Production Deployment of Fauna

Deploying Fauna as a self-hosted binary installation in a clustered configuration may provide higher availability and scalability for production environments. This approach involves:
- Installing the Fauna binary directly on multiple host machines
- Configuring multi-node, multi-replica clusters for redundancy
- Setting up proper networking, storage, and security
- Implementing monitoring, backup, and disaster recovery procedures

## Prerequisites

**System Requirements (Per Node):**
- Enterprise-grade hardware (8+ cores, 32GB+ RAM recommended)
- Fast SSD storage
- High-speed, low-latency network connectivity between nodes
- Linux-based operating system (Rocky Linux 9.4+ recommended)
- Java Runtime Environment 17 installed (OpenJDK or AWS Corretto)
- Properly configured NTP service (chronyd recommended)

When deploying to AWS, we currently recommend using `i4i.2xlarge` instances which have 8 cores, 64GB RAM and 1875GB of ephemeral NVMe storage. You may be able to use another instance size/type such as `i4i.xlarge`, however other instances types are untested.

Running with EBS as the underlying storage device is untested. 

## Cluster Topology Configuration

Fauna is designed to be deployed in a clustered configuration. Individual nodes within a cluster are organized by replica. There are two types of replicas:
- compute only
- data+log

Compute only replicas do not contain data and serve only to act as transaction coordinators, executing FQL queries.

Data+Log replicas contain a full copy of the cluster dataset distributed across all member nodes, participate in the global log, and process scan-based background tasks. Data+Log replicas can also execute FQL queries.

### Single Region Topology 

The minimal configuration is 3 data+log replicas comprising 1 node each, however the recommended smallest configuration for production is 3 data+log replicas, plus 1 replica comprising at least 2 compute nodes.

For single region deployments, each data+log replica should exist in a separate Availability Zone. Compute nodes as well should be distributed across multiple Availability Zones.

### Multi Region Topology

For a multi-region deployment, it is recommended to deploy Fauna into 3 regions, each with a data+log replica and a corresponding compute replica.


## API Endpoint & Load Balancing Configuration

We recommend putting your Fauna cluster behind a load balancer such as AWS ALB for serving traffic. Configure your ALB for TLS termination.

If your Fauna cluster is configured with separate compute and data tiers, only the compute tier should be added to the load balancer. If your Fauna cluster is not configured with a separate compute tier, then all nodes should be added to the load balancer.

### Node health check

Use `/ping?scope=node` as the load balancer's node health check. We recommend a starting check configuration of:
- a polling interval 10 seconds
- a consecutive failure count of 3
- a consecutive readd success count of 1

See [Monitoring Basics](#monitoring-basics) below for details on the `/ping` endpoint. 

### Multi-region cluster load balancing

In a multi-region Fauna cluster, you will need a separate load balancer for each region. In this situation we recommend a solution like AWS Route 53's latency based DNS routing resolution to route clients to the closest region.


## Runbooks

### Cluster Installation and Node Configuration

This runbook assumes an RPM-based Linux distribution that meets the system requirements written out above in [Prerequisites](#prerequisites).

The data directory for nodes will be located at `/media/ephemeral/fauna`. If using EC2 nodes, the ephemeral drive should be mounted at `/media/ephemeral`.

**Setting Up the First Node:**

```bash
# Install prerequisites (example for Rocky Linux)
dnf update
dnf install java-17-openjdk-headless

# Create fauna directories
mkdir -p /opt/fauna/
mkdir -p /media/ephemeral/fauna/{data,log}
useradd fauna -r -d /media/ephemeral/fauna -s /sbin/nologin
chown -R fauna:fauna /media/ephemeral/fauna

# Install Fauna
cd /opt/fauna
curl https://path/to/fauna-2025-02.tar.gz -O
tar -xzf fauna-2025-02.tar.gz

# Configure the first node
cat > /opt/fauna/faunadb.yml << EOF
auth_root_key: <PRE-SHARED KEY> 
# or generate a root key hash. See "Generate an auth_root_key_hash" 
# auth_root_key_hash: <PRE-SHARED KEY HASH>
cluster_name: production_cluster
storage_data_path: /media/ephemeral/fauna/data
log_path: /media/ephemeral/fauna/log
network_listen_address: <node1-private-ip>
network_broadcast_address: <node1-private-ip>
network_admin_http_address: 127.0.0.1
network_coordinator_http_address: 0.0.0.0
EOF

# Create systemd service file
cat > /etc/systemd/system/faunadb.service << EOF
[Unit]
Description=FaunaDB Server
After=network.target

[Service]
User=fauna
Group=fauna
ExecStart=/opt/fauna/bin/faunadb -c /opt/fauna/faunadb.yml
Restart=on-failure
LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
EOF

# Start the service
systemctl daemon-reload
systemctl enable faunadb
systemctl start faunadb

# Initialize the cluster
cd /opt/fauna/bin
faunadb-admin init -r replica_1
```

**Adding Additional Nodes to the Same Replica:**

```bash
# On each additional node in the same data center/replica:
# Configure similar to the first node, but with unique IP addresses
# Then join the cluster:
cd /opt/fauna/bin
faunadb-admin join <first-node-ip> -r replica_1
```

**Setting Up Additional Replicas:**

```bash
# For nodes in separate data centers/replicas:
# Configure with unique replica names (replica_2, replica_3, etc.)
# Start Fauna and join the cluster:
cd /opt/fauna/bin
faunadb-admin join <first-node-ip> -r replica_2
```

**Establishing Multi-Replica Replication:**

```bash
# After all replicas have joined the cluster, run on the first node of the primary replica:
cd /opt/fauna/bin
faunadb-admin update-replica data+log replica_1 replica_2 replica_3

# Verify replication status
faunadb-admin status
```

**Add dedicated compute replicas:**

```bash
# After adding compute replicas compute_1, compute_2, and compute_3, run on the first node:
cd /opt/fauna/bin
faunadb-admin update-replica compute replica_1 replica_2 replica_3

# Verify replication status
faunadb-admin status
```

### Configure Peer-to-peer Encryption

By default peer-to-peer communication between nodes in a Fauna cluster is sent in the clear. It is strongly recommended to configure peer-to-peer TLS in order to protect this communication.

The most basic configuration uses a PEM encoded private key.

Generate a private key using openssl:

```bash
openssl genrsa -passout "pass:<key password>" -out /opt/fauna/peer_key.pem 2048
chown fauna:fauna /opt/fauna/peer_key.pem 
```

Add the key password and path to `faunadb.yml`:

```yaml
peer_encryption_level: all
peer_encryption_key_file: "/opt/fauna/peer_key.pem"
peer_encryption_password: "<key password>" 
```


### Generate an `auth_root_key_hash`

Once the `faunadb` process is started, you may generate a hash value for the `auth_root_key_hash` config parameter, which is safer than the cleartext `auth_root_key` parameter.

```bash
faunadb-admin hash-key <PRE-SHARED KEY>
```

Save the resulting hash value in the `auth_root_key_hash` param in your `faunadb.yml` file:

```yaml
auth_root_key_hash: "$2a$05$i.Bayh0ugOQJ.xqrDNBEdOF.H71lrwxLOaSJfZII/kxK6SSmB9qxC"
```

**NOTE:** If you are using `auth_key_root_hash`, then when running faunadb-admin, you will need to manually provide the root admin key using the `--key/-k` CLI option, or the `ADMIN_KEY` environment variable.

### Monitoring Basics

The fauna process has a ping utility endpoint which provides a basic check of availability of the system. By default, `/ping` will return 200 OK if the cluster is fully available for writes from this node.

```bash
curl http://localhost:8443/ping
```

The ping endpoint respects a "scope" query parameter which allows checking of different levels of availability:

- `node`: This node and its subsystems are live.
- `read`: The cluster is available for reads from this node's perspective.
- `write`: The cluster is fully available for writes from this node's perspective.


#### DataDog/StatsD monitoring

The Fauna process can send metrics to a local StatsD sink. We using DataDog, and installing the DD Agent locally on each node. By default, the Fauna process will send metrics to localhost:8126

#### Important metrics

The following are the most useful metrics the Fauna process emits:

- `HTTP.Responses.XXX`: A count of request responses by status code.
- `Queries.Processed`: A count of total queries processed.
- `Query.Time`: Overall query processing time.
- `Transaction.Apply.Latency.*:` Overall latency of the transaction pipeline.
- `Storage.Transactions.Contended`: A count of transactions which retried or failed due to contention.
- `Storage.Transactions.Schema.Contention`: A count of transactions which retried or failed due to concurrent schema modification.
- `JVM.GC.*`: Metrics associated with JVM performance. NB these metrics are _cumulative_ from the start of the process. They can be passed through a rate derivative function such as `per_minute`.


### Cluster Backup Strategy

#### Creating Scheduled Backups:

```bash
# Create systemd service and timer for scheduled backups
cat > /etc/systemd/system/fauna-backup.service << EOF
[Unit]
Description=Fauna Database Backup
After=network.target

[Service]
Type=oneshot
Environment="S3_AWS_REGION=us-east-1"
Environment="S3_AWS_PREFIX="s3://my/bucket/and/path"
Environment="SNAPSHOT_ROOT_DIR=/media/ephemeral/fauna/snapshots"
ExecStart=/opt/fauna/bin/fauna-backup-s3-upload
User=fauna
Group=fauna

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/fauna-backup.timer << EOF
[Unit]
Description=Run Fauna backup daily

[Timer]
OnCalendar=*-*-* 06:00:00
Persistent=true

[Install]
WantedBy=timers.target
EOF

systemctl daemon-reload
systemctl enable fauna-backup.timer
systemctl start fauna-backup.timer
```


#### Restoring a Cluster from Backup:

```bash
# For emergency restoration of the entire cluster:

# On all nodes - stop Fauna
systemctl stop faunadb

# On all nodes - clear data directories
rm -rf /media/ephemeral/fauna/data/*
chown -R fauna:fauna /media/ephemeral/fauna

# Reinitialize the cluster using the cluster setup instructions above

# Start Fauna on the first node and initialize a new cluster
systemctl start faunadb
cd /opt/fauna/bin
faunadb-admin init -r replica_1

# Start Fauna on other nodes and join the cluster
# On nodes in replica_1
systemctl start faunadb
cd /opt/fauna/bin
faunadb-admin join <first-node-ip> -r replica_1

# On nodes in replica_2
systemctl start faunadb
cd /opt/fauna/bin
faunadb-admin join <first-node-ip> -r replica_2

# On nodes in replica_3
systemctl start faunadb
cd /opt/fauna/bin
faunadb-admin join <first-node-ip> -r replica_3

# From the first node, update replication
faunadb-admin update-replica data+log replica_2
faunadb-admin update-replica data+log replica_3
faunadb-admin update-replication replica_1 replica_2 replica_3

# Load snapshot on the node with the backup
faunadb-admin load-snapshot /path/to/snapshot

# After load completes, update storage version on all nodes
faunadb-admin update-storage-version
```

If a backup is unavailable, it is possible to restore a cluster by directly using node data files. For each path, combine the files from each node, but take care that file names _must_ be deduplicated.

```
/media/ephemeral/fauna/data/data/FAUNA/HistoricalIndex-*
/media/ephemeral/fauna/data/data/FAUNA/LookupStore-*
/media/ephemeral/fauna/data/data/FAUNA/SortedIndex-*
/media/ephemeral/fauna/data/data/FAUNA/Versions-*
```

Create combined directories `HistoricalIndex`, `LookupStore`, `SortedIndex`, and `Versions` under say `/media/ephemeral/combined-data`

Then load the snapshot as above: 

```bash
cd /opt/fauna/bin
faunadb-admin load-snapshot /media/ephemeral/combined-data
```

### Adding a New Node to an Existing Cluster

```bash
# Set up the new node as described earlier

# Join the cluster. Existing nodes in the replica will automatically
# initiate data transfer to the new node.
cd /opt/fauna/bin
faunadb-admin join <existing-node-ip> -r <replica-name>

# Monitor data movement. This may take awhile depending on how
# much data must be transfered.
faunadb-admin movement-status

# IMPORTANT! Once data movement completes, clean up storage.
# This ensures that obsolete data on existing nodes is removed.
faunadb-admin debug clean-storage
```


### Removing a Node from the Cluster

```bash
# Remove the node from the cluster
faunadb-admin remove <host-id> --force

# Monitor data redistribution
faunadb-admin movement-status
```


### Resetting a Corrupted Replica

If a data replica becomes corrupted but other replicas are healthy:

```bash
# Update the problematic replica to be compute-only
cd /opt/fauna/bin
faunadb-admin update-replica compute <replica-name>

# Remove all nodes in the replica
faunadb-admin remove <host-id> --force

# On each node in the replica, stop Fauna and recreate data directories
systemctl stop faunadb
rm -rf /media/ephemeral/fauna/data/*
chown -R fauna:fauna /media/ephemeral/fauna

# Restart Fauna and rejoin the cluster
systemctl start faunadb
cd /opt/fauna/bin
faunadb-admin join <healthy-node-ip> -r <replica-name>

# Promote the replica back to data+log
faunadb-admin update-replica data+log <replica-name>

# Wait for data movement to complete and verify status
faunadb-admin status
faunadb-admin movement-status
```


### Handling High Apply Latency in a Cluster

If experiencing high transaction apply latency:

1. Identify affected nodes using monitoring  
2. Check log segments that might be stuck:
```bash
faunadb-admin status
```
3. Look for log nodes with skipped heartbeats  
4. If one node is stuck:
```bash
systemctl restart faunadb
```
5. If the issue persists, you may need to restart nodes that participate in the same log segment (identifiable from the status output)


### Resetting the Transaction Log

In dire circumstances, the transaction log may be reset. This should only be done as a last resort when a cluster is otherwise unable to process transactions.

1. Shut down fauna on each node:
```bash
systemctl stop faunadb
```

2. Remove transaction log related files on each node (some nodes will not have all files):

```bash
rm -rf /media/ephemeral/fauna/data/scopes/*
rm -rf /media/ephemeral/fauna/data/system/log_topology.*
```

3. Restart fauna on each node:
```bash
systemctl start faunadb
```

Once all nodes are online, the cluster will eventually be able to process transactions again. If the issue persists, the cluster may need to be reset from a backup.
