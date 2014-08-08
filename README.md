GOHA: A lightweight, highly available cluster management library written in Go
==============================================================================

Description
-----------

GOHA is a cluster management plugin library that makes any Go-based service cluster-aware and highly available. It is designed to run in any Go process, providing cluster-awareness both to the process itself as well as to upstream services. Under the hood, GOHA uses a gossip protocol for node discovery, failure detection, and cluster management.

GOHA can be used as a replacement for Zookeeper to create a dependency-free cluster environment for services written in Go.

Features
--------

GOHA is not yet feature-complete. If you are interested in a lightweight clustering library for Go service, please star GOHA. If you are interested in contributing, please drop me a line.

* Distributed, share-nothing architecture
* Simple, sparce feature set based on real devops needs
* Node discovery and failure detection based on gossip protocol
* Internal state change messages via channel
* State exposed externally via JSON REST API
* Extensible: Resource management, rack-awareness, etc. can be layered on top of GOHA

