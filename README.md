# Kafka Streams - REST

Phase: experimentation

Set of utilities to help Kafka Streams to expose and control the state of their applications.

## Modules

- [REST core](./rest-core) Core set of services on top of Kafka Streams abstractions.
- [REST Armeria](./rest-armeria) Armeria HTTP server implementation of core services.
- [Tombstone](./tombstone) Tombstone service to manage Kafka Streams applications state.
