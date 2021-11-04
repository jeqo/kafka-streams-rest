# Rationale

## Design

The Tombstone service is implemented as a Kafka Streams application to consume `KTable`'s source topics and capture keys and timestamps.
Based on these, there is a punctuator scheduled by configuration to evaluate key's timestamps and produce tombstone (`null` values) when TTL condition happens.
The frequency evaluation is based on event time, meaning only when new events arrive, the condition is evaluated—not happening on system wall clock.

Current Kafka Streams topology:

```text
Topologies:
   Sub-topology: 0
    Source: read-table-source (topics: [QUERYABLE_T1])
      --> process-ttl-checks
    Processor: process-ttl-checks (stores: [QUERYABLE_T1])
      --> write-tombstone-back
      <-- read-table-source
    Sink: write-tombstone-back (topic: QUERYABLE_T1)
      <-- process-ttl-checks
```

Where `QUERYABLE_T1` is the source topic name.

### Costs

Having this application deployed, means:

- Adding a Consumer of the KTable source topic, duplicating the consumption of events from the topic.
- A persisting key-value store is managed at the Kafka Streams instance. This will require storage relative to the key cardinality x (key size + timestamp size (`long` value)).
- Tombstone message will increase the source topic size.