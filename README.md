# kitsurai

A distributed, scalable, and fault-tolerant key-value store.

## How it works

A kitsurai cluster is composed of a static list of nodes which might momentarily be unavailable.

### API

Each node exposes an HTTP API with the following endpoints:
- `GET    /`: list the tables.
- `POST   /`: create a table.
- `GET    /{table}`: list the keys in the table.
- `DELETE /{table}`: delete a table.
- `POST   /{table}/{key}`: write a key-value pair.
- `GET    /{table}/{key}`: read a key-value pair.

The client can send its requests to any node in the cluster.
The node will forward the request to the correct nodes and return the result to the client.

A simple client is available to simplify the command line interaction.

### Tables

Each table has four parameters:
- `bandwidth`: the maximum number of bytes that can be processed per second.
- `n`: the replication factor of each key-value pair.
- `r`: the minimum number of nodes that must respond to a read request.
- `w`: the minimum number of nodes that must respond to a write request.

Every node knows all the tables and their parameters.

To create a table the following happens:
- The client connects to a node, which we will call the router, and specifies the table parameters.
- The router generates a uuid for the table.
- The router lists the nodes and asks them to reserve some bandwidth for the table.
- Once the router has enough bandwidth reserved it defines a list of nodes responsible for storing the table.
- The router asks the nodes to commit the table.
- The router returns success and the table uuid to the client.

A gossip protocol ensures that all nodes are in sync with the tables and their parameters.

If a node receives a request to prepare a table, but it does not receive a commit request within a timeout,
 it will mark the table as deleted.
After a table is marked as deleted it cannot be unmarked.
The gossip protocol will ensure that this information is propagated to all nodes.

To delete a table it's enough to mark it as deleted on one node.

### Gossip

Each node initiates a gossip exchange with a random node every second.
With the help of a merkle tree, the nodes quickly find which tables are missing or have been deleted,
then they exchange the tables and their parameters.

### Key-Value pairs

Each key in a table is assigned to `n` nodes from those responsible for the table
respecting the ratios of allocated bandwidth between the nodes.

To do so, we first call `b_i` the bandwidth of node `i` and `Σ` the total allocated bandwidth.
We define a ring of size `Σ` and for each position in the ring we assign a node `i` such that:
- every `n`-long window in the ring contains distinct nodes.
- Every node `i` is assigned to `b_i` positions in the ring.

Given a key we compute it's hash and find the position in the ring.
The key is assigned to the `n` nodes to the right of the key's position in the ring.

Nodes are assigned to the ring iteratively, starting from the first node.
Each node is assigned to `b_i` positions starting from the last assigned and taking a slot every `n`-th position.
If the slot is already taken by another node, we skip it and take its successor.

The construction of such ring can be expensive because the allocated bandwidth can be very large.
The whole construction can be avoided by using a formula to find which node is assigned to a given position in the ring
(see [virt_to_peer](https://virv12.github.io/kitsurai/ktd/state/struct.TableData.html#method.virt_to_peer)).

To read a key each of the `n` nodes is queried and the responses are aggregated.
The first `r` responses available are returned to the client.

To write a key the value is written to each of the `n` nodes.
The write is considered successful if `w` nodes respond.
