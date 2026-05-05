## Kafka Topic Configurations

Kafka topics are categorized into partitions for scalability and replicated across brokers for fault tolerance. Topic configuration parameters play a critical role in determining the behavior of topics in terms of performance, durability, and availability.

### Key Points for CCDAK on Topics

- **Durability and Fault Tolerance**: Configurations like `replication.factor` determine how data is replicated to ensure availability in case of broker failures.
- **Scalability**: The `partition` count of a topic influences how data is distributed across the cluster and impacts parallel processing capabilities.
- **Data Consistency**: The `acks` setting affects how producers receive acknowledgments from brokers, impacting data consistency guarantees.
- **Order Guarantee:** Kafka guarantees that any consumer of a given topic-partition will always read that partition's events in exactly the same order as they were written.
    - If maintaining strict order within a partition is critical, set max.in.flight.requests.per.connection to 1. This ensures that while a message is retrying, no other messages can be sent that might overtake the retried message. (Unless `enable.idempotence` is enabled, as it prevents message reordering caused by retries.)
- **Partition-Specific Order:** Order is guaranteed only within a partition, not across partitions.
    - Repartitioning can disrupt the order of messages in Kafka, primarily because it changes the way records are assigned to partitions.
- **Variable Message Count:** Partitions do not need to have the same number of messages.
- **Partition-Specific Offsets:** Offsets only have meaning within a specific partition.
- **Data Retention:** Messages are kept only for a limited time in Kafka, with the default being one week.
- **Immutability:** Once data is written to a partition, it cannot be changed.
- **Partition Assignment:** Data is assigned randomly to a partition unless a key is provided.

After adding partitions later, it cannot be guaranteed that old messages will be on the same partition as new messages with the same key.

### Important Topic Properties

#### `acks`
- **Scope**: Producer
- **Description**: Determines the number of acknowledgments the producer requires from brokers before considering a request complete. Valid values are `0`, `1`, or `all`.
- **Impact**: This setting impacts data durability and producer throughput. Setting `acks=all` ensures higher data safety as it waits for all in-sync replicas to acknowledge. However, this may lower throughput compared to `acks=1` or `acks=0`, where the latter offers the highest throughput but with potential data loss.

#### `replication.factor`
- **Scope**: Topic
- **Description**: Specifies the number of copies (replicas) of a topic to maintain across the cluster.
- **Default Value**: This is a topic-level configuration set at the time of topic creation and doesn't have a universal default. It often defaults to the cluster's `default.replication.factor`.
- **Impact**: A higher replication factor increases data availability and fault tolerance but requires more disk space and network bandwidth. It is critical for ensuring that even if some brokers are down, your data remains accessible.

#### `partitions`
- **Scope**: Topic
- **Description**: Determines the number of partitions within a topic. Partitions are the basic unit of parallelism in Kafka, with each partition being independently consumed.
- **Default Value**: This is set at the time of topic creation and typically defaults to the cluster's `num.partitions` setting.
- **Impact**: More partitions allow greater parallel processing of data but can increase the overhead on the Kafka cluster and Zookeeper. Finding the right balance is key to optimizing performance and resource utilization.

### Essential Kafka Topic Configuration Parameters

#### Durability and Fault Tolerance

- **`replication.factor`**: Defines the number of replicated copies of a topic across the cluster, enhancing data availability and fault tolerance. The actual number can be set per topic and is crucial for maintaining data integrity in the event of broker failures.

#### Scalability Through Partitioning

- **`partitions`**: Controls the partition count for a topic, directly influencing data distribution and parallel processing capabilities across consumers. More partitions support higher parallelism but may increase cluster management overhead.

#### Data Consistency and Throughput

- **`acks`**: Configures acknowledgment requirements from brokers to producers, balancing between data consistency and throughput. Values range from `0` (fire and forget), `1` (leader acknowledgment), to `all` (full ISR acknowledgment).

### Advanced Topic Configurations for Fine-Tuning

#### Log Compaction and Retention

- **`cleanup.policy`**: Supports log compaction (`"compact"`) to retain at least the last known value for each key within a topic, crucial for stateful applications.
    - The default `cleanup.policy` for Kafka topics is "delete". (Kafka will delete records that are older than the retention period specified by `retention.ms`.)
    - Use `kafka-topics.sh` CLI tool or Admin API to change the policy for existing topics.
    - The `delete` policy might also be used in tandem to prevent the state store from growing indefinitely, especially when windowed operations are involved. (Use `"compact,delete"` )
    - Kafka Streams often requires that the latest state be recoverable even after restarts or rebalancing, making compaction a necessity.
- **`min.cleanable.dirty.ratio`**: Determines the ratio of log segments eligible for compaction. Lower values trigger compaction sooner, helping maintain a cleaner log with less overhead.

- ** Exemple 
# 📘 Kafka Streams — State Store, Changelog & Compaction

## 🧾 1. Context

We use Apache Kafka and Kafka Streams to process a stream of events.

### Source Topic Example

    orders
    ------------------------
    user1 → 20
    user1 → 15
    user2 → 30
    user1 → 10

**Goal:**

👉 Compute the **total amount per user**

---

## ⚙️ 2. Kafka Streams Code

    KTable<String, Long> totalByUser =
        orders.groupByKey()
              .aggregate(
                  () -> 0L,
                  (user, amount, total) -> total + amount
              );

---

## 🧠 3. State Store (Local State)

The **state store** keeps the current state:

    user1 → 45
    user2 → 30

### 🔁 How it works

For each incoming record:

    current_total = state_store[user]
    new_total = current_total + value
    state_store[user] = new_total

---

## ❗ Why no history in the state store?

The state store does **not** keep history because:

👉 It stores only the **latest state**

### Analogy

- History = bank transactions
- State store = current balance

---

## 📤 4. Target Topic (Output)

    orders-total-by-user
    ------------------------
    user1 → 20
    user1 → 35
    user2 → 30
    user1 → 45

👉 This is a **stream of updates**, not just the final result.

---

## 📦 5. Changelog Topic (Backup)

Kafka Streams automatically creates an internal topic:

    app-store-changelog

### Before compaction

    user1 → 20
    user1 → 35
    user2 → 30
    user1 → 45

### After compaction

    user1 → 45
    user2 → 30

---

## 🔁 6. Component Roles

| Component        | Role                          |
|------------------|-------------------------------|
| Source Topic     | Full event history            |
| State Store      | Current state (fast access)   |
| Target Topic     | Produced results (updates)    |
| Changelog Topic  | Backup of the state store     |

---

## 🔄 7. Why do we need a State Store?

### Without a state store:

- No memory between events
- Impossible to compute:
  - count
  - aggregate
  - joins

### With a state store:

- Incremental computation
- High performance

---

## ⚠️ 8. Why not use the target topic as state?

Because:

- Kafka is **append-only**, not a database
- No fast key lookup
- Requires full scan (very slow)
- Not designed for random access

---

## 🧹 9. cleanup.policy

### Changelog Topic

    cleanup.policy=compact

✅ Default  
✅ Required for state recovery

---

### Target Topic

You choose based on your use case:

#### Option 1: delete

    cleanup.policy=delete

👉 Keep history for a limited time

#### Option 2: compact

    cleanup.policy=compact

👉 Keep only the latest value per key

#### Option 3: compact + delete

    cleanup.policy=compact,delete

👉 Combine both strategies

---

## 🔧 10. End-to-End Flow

    Source Topic (orders)
            ↓
    Kafka Streams Processing
            ↓
    State Store (current state)
            ↓
    Target Topic (results stream)
            ↓
    Changelog Topic (backup, compacted)

---

## 🧠 Simple Summary

- Kafka = memory of the past
- State Store = memory of the present
- Changelog = backup
- Compaction = keep latest state only

#### Segment Management and Efficiency

- **`segment.ms`**: Configures the time Kafka waits before closing the current log segment and starting a new one. Segment management affects storage and can impact log compaction and retention behavior.
- # Kafka — Segment Management, Compaction et Retention

# Kafka — Segment Management, Compaction, and Retention

## 🧩 Segments in Kafka
- Messages in a Kafka topic are stored in **segments** (files).
- A segment contains **multiple messages**.
- Kafka writes to an active segment, then closes it and creates a new one.
- `segment.ms` defines **how long before a segment is rotated**.

---

## 🔁 Log Compaction (`cleanup.policy=compact`)

### Goal:
Keep only the **latest value for each key**.

### How it works:
- Kafka scans segments
- Identifies the **latest occurrence of each key**
- Removes older versions

### Important property:
- Kafka **does not delete a segment if it still contains useful data**
- So the latest value of a key is **protected**

✅ Result:
- Behavior similar to a key-value database
- The latest value is kept indefinitely

---

## ⚠️ Limitation of `compact` only

- Keys that are no longer updated remain **forever**
- The topic can **grow without limit**
- Long-term storage issue

---

## 🧹 Retention (`cleanup.policy=delete`)

### Goal:
Delete data based on **time or size**

Example:
- `retention.ms = 7 days`
- → older data is deleted

---

## ⚖️ Combination: `cleanup.policy=compact,delete`

### Behavior:
- Kafka keeps the **latest value per key**
- BUT only for a certain amount of time

👉 If a key is not updated:
- Its latest value can be deleted after expiration

⚠️ Important:
- Yes, **the latest value of a key can be deleted**
- This is not an error, it is a **configuration choice**

---

## 🧠 Why use `compact,delete`?

### 1. Temporary data (TTL)
- User sessions
- Cache
- Ephemeral state

👉 We want:
- the latest value ✔️
- but not forever ✔️

---

### 2. Data size control
- Avoid infinite growth
- Clean up inactive keys

---

### 3. Storage / consistency trade-off
- `compact` → maximum consistency but unlimited storage
- `compact,delete` → controlled storage but possible data loss

---

## 🔥 Summary

| Mode              | Latest value guaranteed | Lifetime   | Use case                |
|-------------------|-------------------------|------------|-------------------------|
| `compact`         | ✅ Yes                   | ∞          | critical state, config |
| `compact,delete`  | ⚠️ No                   | limited    | cache, sessions, IoT   |

---

## 💡 Conclusion

- Kafka does not blindly delete segments
- Compaction protects the latest value… unless retention is enabled
- `compact,delete` = **time-based consistency + storage control**


#### Timestamps and Ordering

- **`message.timestamp.type`**: Defines whether Kafka should use the message creation time (`CreateTime`) or the time of log append (`LogAppendTime`). This setting can impact message ordering and log retention policies.

### Key Considerations for Message Key and Size

#### Message Key Usage

- Utilizing a message key (`key!=null`) ensures that messages with the same key are always sent to the same partition, facilitating message ordering per key. However, modifying the partition count can disrupt this consistency.

#### Managing Message Sizes

- Kafka's default message size limit is 1MB. Exceeding this without proper configuration adjustments (`message.max.bytes`, `replica.fetch.max.bytes`, `fetch.message.max.bytes`) will result in a `MessageSizeTooLargeException`.