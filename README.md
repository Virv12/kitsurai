# kitsurai

A distributed, scalable, and fault-tolerant key-value store.

## Description

A kitsurai cluster is composed of a static list of nodes which might momentarily be unavailable.

### API

Each node exposes an HTTP API with the following endpoints:
- `GET    /`: list all available tables.
- `POST   /`: create a new table.
- `DELETE /{table}`: delete the table.
- `GET    /{table}`: list all keys in the given table.
- `POST   /{table}/{key}`: write a key-value pair.
- `GET    /{table}/{key}`: read a key-value pair.

The client can send its requests to any node in the cluster, and it will be the _router_ for the request.
The router will forward the request to the correct nodes and return the result to the client.

> The routing step could be skipped by an intelligent client, if provided with enough metadata.

A simple client, `ktc`, is available to simplify the command line interaction.
Run `ktc --help` for more information.

### Tables

Each table has four parameters:
- `bandwidth` or `b`: the maximum number of bytes that can be processed per second;
- `n`: the replication factor of each key-value pair;
- `r`: the minimum number of nodes that must respond to a read request;
- `w`: the minimum number of nodes that must respond to a write request.

Every node knows all the tables and their parameters, this is managed by the [Gossip Protocol](#gossip).
This property is what allows any node to become the router for any client request.

#### Creation and Deletion

To create a table the following happens:
1. the client sends the table parameters;
2. the router generates an uuid for the table and computes the actual bandwidth `B = b * n` to account for replication;
3. the router asks the other nodes for available bandwidth to allocate on the new table, 
    while being careful to not allocate more than `B / n` on a single node, 
    guaranteeing that the table will be actually replicated `n` times;
4. once the router has enough bandwidth (`>= B`)
    reserved it defines the list of nodes responsible for storing the table and their effective quota;
5. the router asks the nodes to commit the table;
6. if everyone has commited the table, the router returns the new table's uuid to the client.

Step 3 is called the _preparation_ step while step 5 is called the _commit_ step.

Tables can be in 3 states; prepared, committed or created and deleted; 
and can only _grow_ in state, i.e.: a table cannot become prepared after it has been created. 

If a node receives a preparation request for some new table, 
 but does not receive a commit within a (configurable) timeout,
 it will mark the table as deleted.

The last two features ensure that should something go wrong during table creation, 
 the table will quickly become deleted on all nodes thanks to the gossip protocol.

It follows that marking a table as deleted on one node 
 is enough to (eventually) delete the table in the entire cluster.

#### Metadata Storage

Table metadata is stored in a special table in the underlying store:
metadata is stored on the UUIDv8 built from the bytes `kitsuraimetadata`, 
while any user table is stored under a UUIDv7, ensuring no conflict.

### Gossip

Each node initiates a gossip exchange with a random node every second.
With the help of a merkle tree, nodes can quickly find which tables are different
and exchange tables status and parameters.

if the chosen node is offline then ...TODO?

### Key-Value Pairs

Each key in a table is assigned to `n` nodes from those responsible for the table,
respecting the ratio of allocated bandwidth on the nodes.

To do so, we first call `b_i` the bandwidth of node `i` and `Σ` the total allocated bandwidth.
We define a ring of size `Σ` and for each position in the ring we assign a node `i` such that:
- every `n`-long window in the ring contains distinct nodes;
- every node `i` is assigned to `b_i` positions in the ring.

Given a key we compute its hash (with [XXH3](https://xxhash.com) a fast non-crypto hash algorithm)
 and find the position in the ring.
The key is assigned to the `n` nodes to the right of the key's position in the ring.

Nodes are assigned to the ring iteratively, starting from the first node.
Each node is assigned to `b_i` positions starting from the last assigned and taking a slot every `n`-th position.
If the slot is already taken by another node, we skip it and take its successor.

As the allocated bandwidth could be considerably high the construction of a ring 
 that meets the above criteria could be expensive.
This construction can be avoided by using a formula
 to find which node is assigned to a given position in the ring,
 see [virt_to_peer](https://virv12.github.io/kitsurai/ktd/state/struct.TableData.html#method.virt_to_peer).

To read (or write) a key each of the `n` nodes is queried and the responses aggregated.
The first `r` (or `w`) responses available are returned to the client.

#### Availability Zones

To guarantee the database's availability each node can be tagged with a string, 
 identifying its availability zone, AZ from now.
To allocate each table's key in `n` AZs the previous allocation algorithm requires only a slight change:
- the router shall not take more than `b / n` bandwidth from nodes in a single AZ;
- nodes should be assigned in the ring ordered by their AZ.

### Remote Procedure Calls

Remote procedures are executed with the following protocol:
1. A opens a TCP connection to B;
2. A sends the serialized RPC request;
3. A closes its TCP stream (sending `FIN` to B);
4. B waits until it has received all the available data;
5. B deserializes and executes the RPC;
6. B sends the RPC's response back and closes the connection;
7. A waits until it has received all of the response and returns the value to the callee.

## Future Features and Limitations
The current implementation does not provide any method to:
- resolve read conflicts;
- choose table names.

Both of these are intentionally not implemented as locking a solution to one of these problems
 requires strengthening the assumptions and increases the code complexity.

Nonetheless, both of these features could be implemented on top of the current interface as HTTP proxies.
To resolve read conflicts a proxy could inject in every write a timestamp 
 and on reads pick the latest version available to both update the out of date nodes and return to the client.  
For the latter another proxy could use the database itself as a storage for table names or an external database,
 to translate any query's table on the fly.

Lastly physical disks are not accounted and the system 
 could be generalized to support multiple bandwidths on a single node.

A limitation of the table allocation algorithm is its incapacity to manage the nodes' capacity.
 A table with little bandwidth requirements but large capacity needs could potentially brake the system.
A mitigation for this problem could change the way the bandwidth's unit to represent
 some ratio between available capacity and actual speed, coupled with a limitation in key number and sizes.

Currently, the cluster cannot change its nodes while it is running, 
 nor can a table migrate should a node persistently fail.
It should be possible to add both of these features 
 where some way to migrate to be implemented.  

## Benchmarks

TODO!