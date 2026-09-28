# 🚀 Day 24 - Stateless vs Stateful Streaming

## 📌 Project Overview

Day 24 demonstrates the difference between **stateless and stateful stream processing** using Apache Spark DStreams.

The project simulates a bank transaction stream where account IDs are received through a TCP socket using `nc`.

The application compares:

- Current-batch transaction counts
- Running transaction counts accumulated across batches

---

## 🎯 Objectives

- Implement stateless streaming transformations
- Understand stateful stream processing
- Track running counts by bank account
- Compare current-batch results with accumulated state
- Demonstrate state persistence across micro-batches
- Use a real-time TCP socket stream
- Maintain running transaction counts per bank account

---

## 🛠️ Technologies Used

- Apache Spark 3.5.3
- Spark Streaming / DStreams
- Scala 2.12.18
- SBT
- Ubuntu/Linux
- TCP Socket
- Netcat (`nc`)
- Git & GitHub

---

## 📁 Project Structure

```text
day24-spark/
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── Day24StatefulStreaming.scala
│
├── .gitignore
├── build.sbt
└── README.md
```

---

## 🔄 Streaming Architecture

```text
Bank Account Transactions
            ↓
           nc
            ↓
    TCP Socket : 9999
            ↓
      Spark DStream
            ↓
      ┌───────────┐
      │ Stateless │
      └───────────┘
            ↓
 Current Batch Count

            +

      ┌───────────┐
      │ Stateful  │
      └───────────┘
            ↓
 Running Count
```

---

## ⏱️ Micro-Batch Processing

The application uses a **5-second batch interval**.

```text
Batch 1 → Current transactions
Batch 2 → New transactions
Batch 3 → New transactions
...
```

Stateless processing considers only the current batch.

Stateful processing remembers the results from previous batches and maintains a running total.

---

# 🔹 Stateless Processing

Stateless processing does not remember information from previous micro-batches.

The application calculates transaction counts only for the current batch.

```scala
val statelessCounts = accounts
  .map(account => (account, 1))
  .reduceByKey(_ + _)
```

For example, if the current batch contains:

```text
ACC001
ACC001
ACC002
```

The stateless result is:

```text
ACC001 -> 2
ACC002 -> 1
```

If the next batch contains:

```text
ACC001
ACC003
```

the stateless result becomes:

```text
ACC001 -> 1
ACC003 -> 1
```

The previous batch is not included.

---

# 🔹 Stateful Processing

Stateful processing maintains information across multiple micro-batches.

The project uses:

```scala
updateStateByKey()
```

to maintain the running transaction count for every bank account.

The previous state is combined with the current batch:

```scala
val previousCount = previousState.getOrElse(0)
val currentBatchCount = newValues.sum

Some(previousCount + currentBatchCount)
```

For example:

### Batch 1

```text
ACC001
ACC001
ACC002
```

Running state:

```text
ACC001 -> 2
ACC002 -> 1
```

### Batch 2

```text
ACC001
ACC003
```

Running state becomes:

```text
ACC001 -> 3
ACC002 -> 1
ACC003 -> 1
```

The previous state is preserved.

---

## 📊 Stateless vs Stateful

| Feature | Stateless | Stateful |
|---|---|---|
| Previous batch remembered | ❌ No | ✅ Yes |
| Current batch processed | ✅ Yes | ✅ Yes |
| Running count | ❌ No | ✅ Yes |
| State maintained | ❌ No | ✅ Yes |
| Example | Current batch count | Running account count |

---

## 🔌 Socket Input

The application reads account IDs from:

```text
localhost:9999
```

The Spark application uses:

```scala
val lines = ssc.socketTextStream("localhost", 9999)
```

---

## 🖥️ Running the Application

### Step 1 - Start Spark

Open Terminal 1:

```bash
cd ~/day24-spark
sbt -error run
```

The application displays:

```text
==========================================
       DAY 24 - STREAMING STARTED
==========================================
Batch Interval : 5 seconds
Socket         : localhost:9999
Status         : WAITING FOR DATA
==========================================
```

---

### Step 2 - Start Netcat

Open Terminal 2:

```bash
nc -lk 9999
```

---

### Step 3 - Send Transactions

Send account IDs:

```text
ACC001
ACC001
ACC002
```

The application processes them during the next 5-second micro-batch.

---

## 📈 Sample Output

### Stateless Output

```text
==========================================
          STATELESS PROCESSING
==========================================
Current batch transaction counts:
ACC001 -> 2
ACC002 -> 1
==========================================
```

### Stateful Output

```text
==========================================
           STATEFUL PROCESSING
==========================================
Running transaction counts:
ACC001 -> 2
ACC002 -> 1
==========================================
```

After another batch:

```text
ACC001
ACC003
```

Stateless output:

```text
==========================================
          STATELESS PROCESSING
==========================================
Current batch transaction counts:
ACC001 -> 1
ACC003 -> 1
==========================================
```

Stateful output:

```text
==========================================
           STATEFUL PROCESSING
==========================================
Running transaction counts:
ACC001 -> 3
ACC002 -> 1
ACC003 -> 1
==========================================
```

---

## 🏦 Real-World Scenario

This project represents a simple **bank transaction monitoring system**.

A bank can receive transactions continuously:

```text
ACC001
ACC002
ACC001
ACC003
ACC001
```

Stateless processing can determine the number of transactions received in the current interval.

Stateful processing can maintain the total transaction count for each account over time.

This concept can be extended to:

- Bank transaction monitoring
- Account activity tracking
- Fraud detection
- Customer activity monitoring
- Real-time analytics
- Event processing

---

## 🧠 Key Concepts Learned

### DStream

A DStream represents a continuous stream of data divided into micro-batches.

### Stateless Processing

Each micro-batch is processed independently without remembering previous batches.

### Stateful Processing

Information from previous batches is maintained and combined with new data.

### `updateStateByKey()`

Used to maintain state across multiple batches.

### Checkpointing

The application uses a checkpoint directory for stateful streaming:

```text
/tmp/day24-checkpoint
```

Checkpointing allows Spark Streaming to store state information needed for stateful processing.

---

## 🔄 Processing Comparison

```text
                    STREAM
                       ↓
              ┌───────────────┐
              │  Micro-batch  │
              └───────────────┘
                       ↓
             ┌──────────────────┐
             │                  │
             ↓                  ↓
        STATELESS           STATEFUL
             ↓                  ↓
      Current Batch       Previous State
             ↓                  +
       Batch Count         Current Batch
                                ↓
                         Running Count
```

---

## ✅ Day 24 Checklist

- [x] Create StreamingContext
- [x] Configure 5-second batch interval
- [x] Read TCP socket stream
- [x] Use `nc` for input
- [x] Implement stateless processing
- [x] Implement stateful processing
- [x] Use `updateStateByKey()`
- [x] Track running counts by account
- [x] Compare current batch with accumulated state
- [x] Demonstrate multiple micro-batches
- [x] Understand checkpointing
- [x] Verify streaming output

---

⭐ **Day 24 – Stateless vs Stateful Streaming**
