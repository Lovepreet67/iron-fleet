# Iron Fleet - Rust Implementation of Gossip Glomers Challenge

This repository contains my Rust implementation of the [Gossip Glomers](https://fly.io/dist-sys/) challenges. The challenges are tested using [Maelstrom](https://github.com/jepsen-io/maelstrom), a powerful distributed system testing framework.

## Introduction

To get started with this challenge, we first have to implement the required protocol for Rust. Maelstrom uses a basic JSON-based protocol using stdin and stdout as a transmission channel.

To implement this, I have isolated its implementation in the `maelstrom` module, which runs two threads—one for receiving and one for sending messages. Communication with these threads is done using MPSC channels, which are stored in the `maelstrom` struct.

Each challenge is placed in its respective directory, containing the Rust solution and necessary configuration files.

## Challenge Details

Each challenge follows a structured testing approach using Maelstrom. Below are instructions for running each challenge:

### 1. Echo Challenge
This challenge is to test communication. The node will receive an `echo` message and must reply with an `echo_ok` message.

```sh
maelstrom test -w echo --bin target/release/iron-fleet --node-count 1 --time-limit 10
```

### 2. Unique ID Generation
In this challenge, the node will receive a `generate` message and should return a `generate_ok` message containing a unique ID in the overall system. This is implemented by maintaining a counter on each node and returning an ID using `node_id + local_counter`. Every time an ID is generated, we increment the local counter. Since this only works with the local state of the node, we don't have to worry about network partitions.

```sh
maelstrom test -w unique-ids --bin target/release/iron-fleet --node-count 3 --rate 100 --time-limit 10 --nemesis partition
```

### 3. Broadcast Challenge
This challenge is about the Gossip protocol, which is used to propagate messages in the system. The node will receive a `broadcast` message containing a unique number and must broadcast it to other nodes in the system.

Considering the uniqueness property, we can create a CRDT (Conflict-Free Replicated Data Type) and share it using the gossip message. On receiving the gossip message, the receiver can easily merge the received state with its own messages. For gossiping, I have used a separate thread that utilizes the `rand` crate to choose random neighbors and send the current state to them. The gossip thread repeats this process at a certain interval.

```sh
maelstrom test -w broadcast --bin target/release/iron-fleet --node-count 5 --time-limit 20 --rate 10
```

### 4. Grow-only Counter
This challenge is somewhat similar to the broadcast challenge. The node will receive an `add` message containing a delta value with which we must increment the counter. We must refactor the CRDT to support this by assigning a unique identifier to each `add` message and then merging them through gossip.

```sh
maelstrom test -w grow-only-counter --bin target/release/iron-fleet --node-count 3 --rate 100 --time-limit 20
```

### 5. Kafka-style Log Challenge
In this challenge, we have to implement a node that can store messages, store committed offsets, and replicate messages. This challenge takes a different approach than the previous ones. Instead of gossip, we use message forwarding, which is somewhat similar to a write-majority quorum (not completely but relatable).

When a node receives a write message, it forwards it to other nodes. Since this implementation is not network-partition tolerant, we assume that every message sent will be received. This challenge also uses the `lin-kv` service to store the latest offset for a topic. To reduce message frequency when handling offsets, I have used a locking mechanism, ensuring that only one node updates the offset at a time and only unlocks it after replying to maintain the monotonicity of offsets.

```sh
maelstrom test -w kafka --bin target/release/iron-fleet --node-count 2 --time-limit 20 --rate 1000
```

### 6. Totally-Available Transactions
This challenge is similar to the last challenge. In this challenge, the node will receive transactions and must maintain a key-value store. The system needs to support **read-committed** consistency, which is achieved using a form of snapshot isolation. This implementation is network-partition tolerant.

The node supports a `send_sure` method, which resends messages until a response is received. The resend interval increases as the resend count increases.

```sh
maelstrom test -w txn-rw-register --bin target/release/iron-fleet --node-count 2 --rate 1000 --time-limit 20 --concurrency 2n --consistency-models read-committed --availability total --nemesis partition
```

## Building the Rust Binaries

Each challenge has its own directory. To build the binaries:

```sh
cd challenge_directory
cargo build --release
```

The compiled binaries will be located in `target/release/`.

## Notes

- Ensure Maelstrom is correctly set up before running the tests.
- Use `--log-stderr` with Maelstrom to view detailed logs if debugging is needed.
- Modify the node count and rate parameters to simulate different conditions.

Happy hacking! 🚀

