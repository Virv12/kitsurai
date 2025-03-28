# kitsurai

A distributed, scalable, and fault-tolerant key-value store.

## How it works

A kitsurai cluster is composed of a static list of nodes which might momentarily be unavailable.

### API

Each node exposes an HTTP API with the following endpoints:
- `GET  /`: list the tables.
- `POST /`: create a table.
- `GET  /{table}`: list the keys in the table.
- `POST /{table}/{key}`: write a key-value pair.
- `GET  /{table}/{key}`: read a key-value pair.

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
- The client connect to a node, which we will call the router, and specifies the table parameters.
- The router generates an uuid for the table.
- The router list the nodes and ask them to reserve some bandwidth for the table.
- Once the router has enough bandwidth reserved it defines a list of nodes responsible for storing the table.
- The router asks the nodes to commit the table.
- The router returns success and the table uuid to the client.

A gossip protocol ensures that all nodes are in sync with the tables and their parameters.

If a node receives a request to prepare a table, but it does not receive a commit request within a timeout,
 it will mark the table as deleted.
After a table is marked as deleted it cannot be recreated.
The gossip protocol will ensure that this information is propagated to all nodes.

To delete a table it's enough to mark it as deleted on one node.

### Key-Value pairs

Each key is assigned to a `n` nodes from those responsible for the table based on the key's hash.
The key-value pair is replicated on each of the `n` nodes.

To read a key each of the `n` nodes is queried and the responses are aggregated.
The first `r` responses available are returned to the client.

To write a key the value is written to each of the `n` nodes.
The write is considered successful if `w` nodes respond.
