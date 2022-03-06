# Kafka Streams - REST

project-phase: experimentation

project-version:

Set of utilities to help Kafka Streams to expose and control the state of their applications.

## Modules

- [REST core](./rest-core) Core set of services on top of Kafka Streams abstractions.
- [REST Armeria](./rest-armeria) Armeria HTTP server implementation of core services.

### REST - Armeria

[Armeria](https://armeria.dev/) is a microservice framework for HTTP, gRPC, and other technologies.
This module uses Armeria HTTP server APIs to expose Kafka Streams application controls, metrics, health-check, metadata, and store information.

## Services

- [Tombstone](./tombstone) Tombstone service to manage Kafka Streams applications state.
