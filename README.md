<img src="https://raw.githubusercontent.com/wiki/obsidiandynamics/transram/images/transram-logo.png" width="90px" alt="logo"/> TransRAM
===
[![Maven release](https://maven-badges.herokuapp.com/maven-central/com.obsidiandynamics.transram/core/badge.svg)](https://search.maven.org/search?q=g:com.obsidiandynamics.transram)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/obsidiandynamics/transram.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/obsidiandynamics/transram/context:java)
[![Gradle build](https://github.com/obsidiandynamics/transram/actions/workflows/master.yml/badge.svg)](https://github.com/obsidiandynamics/transram/actions/workflows/master.yml)
[![codecov](https://codecov.io/gh/obsidiandynamics/transram/branch/master/graph/badge.svg?token=SknOYiH9Wb)](https://codecov.io/gh/obsidiandynamics/transram)

**Transactional memory semantics for the JVM.**

# What does it do?
TransRAM provides _Strict Serializable_ transactional scope over operations on a hash map. It offers the following capabilities and guarantees:

* Conventional atomic commitment and rollback.
* Strict serializable reads and updates.
* Commitment ordering.
* Revealed serial order.
* Key range scanning with predicates.

# Where would I use it?
There are two main use cases. See if you can think of others.

1. **Operating on an in-memory map as if it were a database.** You wish to update multiple items, and the updates must appear atomically. You may even want to roll back changes. You might need performance, without the fine-grained locking or having to reason about concurrency.
2. **Developing concurrency control algorithms.** You are a Computer Scientist (or just a curious engineer) wishing to experiment with new or existing concurrency control algorithms. TransRAM offers a comprehensive set of workloads for verifying both the performance and correctness of algorithms under a broad range of conditions.

# What is this sorcery?
When developing concurrent applications, you might use locks/mutexes, semaphores, etc. Depending on your style, you might also consider coroutines, channels or actors. At any rate, the notion of concurrency is always lingering in some inconvenient way. And concurrent programs tend to be hard to reason about and debug. Yet, when we hammer a relational (or any ACID) database with lots of concurrent transactions, it all just seems to work. (Provided we demarcate transactions correctly and pick a sensible isolation level.) Somehow, the database just sorts itself out.

Databases are remarkably complex, using sophisticated algorithms to rearrange the individual operations of transactions to make them appear as if they are executing serially — in a single thread. (Or abort some of them, if such arrangement is not possible.) In doing so, ACID databases maintain a high degree of concurrency. They largely absolve developers of the intricacies of concurrency control.

What we just discussed corresponds to the 'A' (Atomicity) and 'I' (Isolation) in ACID. Atomicity means that related actions are applied in their entirety or not at all. (I.e., the commit/rollback semantics.) Isolation means that a transaction perceives the database like it is the only transaction operating on it. We invariably arrive at the definition of serializability, which requires elaboration.

Transactions are said to be serializable if their concurrent execution is equivalent to _some_ serial execution of those transactions.

As promising as it may sound, serializability alone isn't as strong a guarantee as it appears. For example, some item _x_, initialised with value 0 could be updated to 1 by transaction T<sub>1</sub> and committed. Later, T<sub>2</sub> might read _x_. No other transactions wrote to _x_. What should T<sub>2</sub> see? If you answered 1, you are among the many (millions of) people that don't (yet) fully comprehend serializability. The right answer: T<sub>2</sub> could see either 0 or 1, as both executions have a valid serial equivalent. If it sees 1, then the serial order is (T<sub>1</sub>, T<sub>2</sub>); otherwise, the serial order is (T<sub>2</sub>, T<sub>1</sub>). A serializable system need only comply with _some_ serial order.

When working with concurrent systems, the isolation level we most often want is _Strict Serializable_. A strict serializable system respects the real-time order of nonconcurrent transactions. I.e., since T<sub>2</sub> commenced strictly after T<sub>1</sub> completed, it must observe T<sub>1</sub>'s effects. When an RDBMS like MySQL or Postgres claims that it is serializable, it is in fact strict serializable. (The reasons relate to the ANSI SQL standard, which we won't delve into here.)

Strict serializability eliminates anomalies that most people tend to care about, but it doesn't reveal the serial order. Sometimes, not knowing the serial order, or worse, naively assuming a serial order, can lead to serious problems. (This was the subject of the recent [Logical Timestamp Skew Anomaly](https://ieeexplore.ieee.org/document/9521923) paper.) Transactions in TransRAM are _Commitment Ordered_, meaning their serialization order corresponds to the order in which the transactions committed. TransRAM conveniently reveals the commitment order, allowing the user to infer the serialization (partial) order.

# Getting started
## 1. Add the Gradle dependency
```groovy
dependencies {
    implementation 'com.obsidiandynamics.transram:core:x.y.z'
}
```

Replace `x.y.z` with the latest release version.

## 2. Implement the `DeepCloneable` interface
The main data structure in TransRAM is a `TransMap`, which is akin to a conventional `java.util.Map`, albeit with a markedly simplified API surface. `TransMap` takes any key that satisfies the canonical `equals`+`hashCode` contract. Because all TransRAM algorithms use multiversion object representations under the hood, map values must implement the `DeepCloneable` interface:

```java
public interface DeepCloneable<SELF> {
  SELF deepClone();
}
```

As the name suggests, the `deepClone()` method should return a complete (deep) copy of the object and all mutable objects that it references. Immutable objects may be exempt from cloning.

## 3. Instantiate a `TransMap`
We can now create a new instance of a `TransMap` implementation, which varies depending on the chosen concurrency algorithm. Our examples use the SS2PL (Strong-Strict Two-Phase Locking) algorithm.

```java
final var map = new Ss2plMap<String, Customer>(new Ss2plMap.Options());
```

Here, we assume the key is a `String` and the value is `Customer` — a simple class that implements `DeepCloneable`.

## 4. Transact over the map
We use the `Transact` helper class to demarcate transactions. The `Transact.run()` method accepts a Lambda function that takes a `TransContext` as its sole argument. `TransContext` is the main interface between user code and the `TransMap` implementation.

For the following examples, assume a `Customer` class with some basic attributes:

```java
class Customer implements DeepCloneable<Customer> {
    private String firstName;

    private String lastName;

    private String email;

    private int balance;

    // constructor and getter/setters not shown

    @Override public Customer deepClone() {
        // return a copy of this object
    }
}
```

### Example 1: Atomically insert values
```java
Transact.over(map).run(ctx -> {
    // Atomically insert a pair of customers.
    ctx.insert("john.citizen", new Customer("John", "Citizen", "john.citizen@local"));
    ctx.insert("jane.citizen", new Customer("Jane", "Citizen", "jane.citizen@local"));

    System.out.format("there are now %d customers%n", ctx.size());
    return Action.COMMIT;        // upon commitment, either both customers will exist or neither
});
```

All read/write operations on a `TransContext` object may throw a `ConcurrentModeFailure` exception, meaning that the currently running transaction could not be serialized. The reason depends on the algorithm used, but in general, not all concurrent executions may be serialized, regardless of the chosen algorithm. As such, the user-supplied Lambda function must be capable of retrying the transaction an arbitrary number of times. The retry logic is implemented in the `Transact` helper, transparent to the user code.

### Example 2: Atomically operate over a group of values
```java
Transact.over(map).run(ctx -> {
    // Atomically move funds between customers.
    final var customer1 = ctx.read("john.citizen");
    final var customer2 = ctx.read("jane.citizen");

    final var amountToTransfer = 100;

    customer1.setBalance(customer1.getBalance() - amountToTransfer);
    customer2.setBalance(customer2.getBalance() + amountToTransfer);
    ctx.update("john.citizen", customer1);
    ctx.update("jane.citizen", customer2);

    if (customer1.getBalance() < 0) {
        System.out.format("Rolling back due to negative funds (%d)%n", customer1.getBalance());
        return Action.ROLLBACK;  // never leave the customer with a –ve balance
    }

    return Action.COMMIT;        // upon commitment, the funds be moved
});
```

### Example 3: Delete a value
```java
Transact.over(map).run(ctx -> {
    // Delete one of the customers.
    ctx.delete("jane.citizen");
    return Action.COMMIT;
});
```

### Example 4: Read all values whose key matches a predicate
```java
Transact.over(map).run(ctx -> {
    // Print all customers whose key ends with '.citizen'.
    final var keys = ctx.keys(key -> key.endsWith(".citizen"));
    for (var key : keys) {
        System.out.format("key: %s, value: %s%n", key, ctx.read(key));
    }
    return Action.ROLLBACK;    // a noncommittal transaction
});
```

# Algorithms
There are two concurrency control algorithms supported in the current release.

1. **SS2PL (Strong-Strict Two-Phase Locking)** — the "textbook" algorithm for implementing strict serializability in universioned databases. In the first phase, locks are acquired and no locks are released. This phase progresses alongside the transaction. Upon commitment or rollback, the second phase is enacted, wherein locks are released and no new locks are acquired. SS2PL may deadlock, which internally forces an abort and the transaction is retried by the `Transact` helper.
2. **SRML v3 (Snapshot Reads with Merge Locking, version 3)** — an experimental algorithm that fulfils reads from a multiversioned snapshot, and then uses locks to merge changes from the local copy to the backing map. Although the merge phase is locking, it can never deadlock, because locks are acquired at most once during a transaction (during commitment) and are ordered to eliminate cycles. Nonetheless, SRML may abort a transaction if it detects an antidependency conflict (which is not possible in SS2PL).

Note, although SS2PL is a universion algorithm (in other words, it does not use multiversion concurrency control to allow nonblocking reads), it still requires that values implement the `DeepCloneable` interface. This is, in general, true of all current and future algorithms. `DeepCloneable` enables the memory-safe separation of the transaction's working copy (i.e., the items it has read and subsequently updated) from the backing store. Without `DeepCloneable`, concurrent transactions would operate on the same instance of an item, even before committing.

Comparing the two algorithms:

|                                                         | SS2PL               | SRML v3                          |
|:--------------------------------------------------------|:--------------------|:---------------------------------|
| Isolation level for committed read/write transactions   | Strict serializable | Strict Serializable              |
| Isolation level for noncommittal read-only transactions | Strict serializable | Serializable                     |
| Supports blind writes                                   | No                  | Yes                              |
| Handling of deleted items                               | Freed               | Replaced with a tombstone        |
| Memory utilisation                                      | Minimal             | Higher, depending on queue depth |

Both algorithms offer strict serializable isolation for all committed transactions.

When a read-only transaction is concluded with a rollback, it is still strict serializable under SS2PL. In other words, SS2PL does not require the user to commit a read-only transaction, unless one needs to determine the serialization order. Under SRML v3, noncommittal read-only transactions are serializable, but are not guaranteed to observe the writes of all strictly preceding transactions.

SRML supports blind writes, wherein a transaction can write to an item, not having read it first, and without inducing a conflict. I.e., if two transactions concurrently write to the same item without reading it, both transactions will succeed — the item will assume the result of one (doesn't matter which) of the writes. SS2PL, on the other hand, blocks one of the transactions until the other completes.

When an item is deleted under SS2PL, the space occupied by the item is freed. SRML v3 replaces the item with a tombstone record, which takes up some space in the map. As such, when inserting and deleting lots of random keys, the memory footprint of SRML will be much larger than that of SS2PL.

On the subject of memory use, SS2PL maintains a single copy of each item, once committed. SRML uses a double-ended queue (a _deque_) under the hood, which accumulates multiple versions of the data item. This allows for nonconflicting snapshot reads, wherein an item may be read without blocking a writer.

# Determining the serialization order
Having committed a transaction, one can extract the (zero-based, monotonic increasing) version of the backing map that was produced as a result. (This is supported in both universion and multiversion algorithms.) Example:

```java
// Change a customer's attribute.
final var completed = Transact.over(map).run(ctx -> {
    final var customer = ctx.read("jane.citizen");
    customer.setEmail("jane.citizen@freemail.org");
    ctx.update("jane.citizen", customer);
    return Action.COMMIT;
});
System.out.format("completed version: %d%n", completed.getVersion());
```

We now define a **conflict**. Two transactions are said to conflict if they both operate on the same data item and at least one of them is a write. Examples:

* T<sub>1</sub> writes to _x_, T<sub>2</sub> writes to _y_. They don't conflict.
* T<sub>3</sub> writes to _x_, T<sub>4</sub> writes to _x_. They conflict.
* T<sub>5</sub> reads from _x_, T<sub>6</sub> writes to _x_. They conflict.
* T<sub>7</sub> reads from _x_, T<sub>8</sub> reads from _x_. They don't conflict.

The serialization graph conforms to a partial order. To determine the serialization partial order from the version number, follow this basic rule:

1. If two transactions don't conflict, they are relatively unordered.
2. If two transactions conflict, their relative order is implied by the inequality of their versions. I.e., T<sub>i</sub>.version < T<sub>j</sub>.version ⇔ T<sub>i</sub> → T<sub>j</sub>, where → denotes a precedence relation.