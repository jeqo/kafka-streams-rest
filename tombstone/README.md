# Kafka Streams Tombstone service

Within Kafka Streams applications (e.g. ksqlDB persistent queries) state is stored and can grow indefinitely when the state store type does not support retention (e.g. KeyValue stores).

This service is aimed to alleviate the challenge of keeping the state stores efficient and remove records after a period of time (TTL) based on its metadata.

This work is inspired on https://kafka-tutorials.confluent.io/schedule-ktable-ttl/kstreams.html.
It wraps this topology and packages it as a side application to be deployed along your Kafka Streams app.
Tombstone messages will be produce on the source topic, so KTables can be updated and older records removed.

## Configuration

| Name | Type | Description | Default |
|------|------|-------------|---------|
| `tombstone.topic` | text | Table source topic. | `<empty>` |
| `tombstone.scan_frequency` | long | Milliseconds representing how often to scan the record timestamps. To be evaluated on event-time, not wall-clock time. | `<empty>` |
| `tombstone.max_age` | long | Milliseconds representing how old a key timestamp could be before a tombstone is produced. | `<empty>` |

## How to use it

To start with, a Kafka Streams application with some Key Value state store or ksqlDB table is required.
This is the state store we want to produce tombstones for.

An example application could have this topology:

```text
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [input])
      --> KTABLE-SOURCE-0000000001
    Processor: KTABLE-SOURCE-0000000001 (stores: [input-table])
      --> KTABLE-TOSTREAM-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KTABLE-TOSTREAM-0000000002 (stores: [])
      --> KSTREAM-SINK-0000000003
      <-- KTABLE-SOURCE-0000000001
    Sink: KSTREAM-SINK-0000000003 (topic: output)
      <-- KTABLE-TOSTREAM-0000000002
```

The store `input-table` is sourced from the topic `input`.

Then, we need to define a records time-to-live (TTL) and how often this condition should be checked.
For testing purposes, we can define scan frequency as 10 seconds, and maximum age as 1 minute.

Set the tombstone service configuration:

```properties
bootstrap.servers=localhost:9092
application.id=tombstone-server-v1
state.dir=target/kafka-streams

tombstone.topic=input
tombstone.scan_frequency=10000
tombstone.max_age=60000
```

And run the application.

You can check the tombstone service store via HTTP API:

```shell
curl -XGET -H 'content-type: application/json; charset=utf-8' 'http://127.0.0.1:8080/stores/key-value/info/'
{
  "storeName": "input",
  "approximateNumEntries": 15
}
```

and check if a particular key exists:

```shell
curl -XGET -H 'content-type: application/json; charset=utf-8' 'http://127.0.0.1:8080/stores/key-value/input/2''
{
  "found": true
}
```

Inspect source topic to check tombstone messages:

```shell
kcat -b localhost:9092 -t QUERYABLE_T1 -C -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\nHeaders: %h\nTimestamp: %T\tPartition: %p\tOffset: %o\n--\n' 
...
Key (1 bytes): 2        
Value (-1 bytes): 
Headers: tombstone=1
Timestamp: 1636050785020        Partition: 0    Offset: 5
...
```

The same approach works for ksqlDB tables, using their source topics.
