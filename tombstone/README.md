# Kafka Streams Tombstone service

Within Kafka Streams applications (e.g. ksqlDB persistent queries) state is stored and can grow indefinitely.
This service is aimed to alleviate the challenge of keeping the state efficient and remove records after a period of time based on its metadata or content.


