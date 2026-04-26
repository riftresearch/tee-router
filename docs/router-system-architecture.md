# Router System Architecture Specification

This document describes the target architecture for the router server system.
The runtime topology separates API and worker responsibilities into different
binaries.

## Current Implementation Notes

- `router-api` is the standard API process. It serves HTTP routes, creates vault
  rows, and records cancellation/refund requests without running background
  refund processing.
- `router-worker` is the standard worker process. It acquires the global
  PostgreSQL worker lease, renews it while active, and processes refund/action
  work only while it holds the lease.
- The worker leadership lease is stored in `router_worker_leases` with
  `owner_id`, `expires_at`, and monotonic `fencing_token`.
- Refund rows also use per-row claim leases so a worker pass can safely claim
  bounded batches of refundable vaults.

## 1. Overview

A multi-provider, high-availability system for processing asynchronous stateful
resources.

- Centralized state machine
- Single logical writer for state progression
- PostgreSQL as source of truth
- Multi-provider redundancy
- TEE-based trust model

## 2. Deployment Topology

### Providers

- Total: 3 independent providers

### Per Provider

- 1 API node
- 1 worker node
- 1 PostgreSQL node

## 3. API Layer

### Characteristics

- Stateless
- Horizontally replicated (3 instances total)
- All nodes active concurrently

### Responsibilities

- Accept client requests
- Validate input
- Insert new resource rows into database
- Return resource ID / status handle

### Non-Responsibilities

- No background processing
- No state progression

### Routing

- Global load balancer routes to healthy API nodes

## 4. Worker Layer

### Instances

- 3 worker nodes (1 per provider)

### Execution Model

- Exactly 1 active worker at any time
- Remaining workers are standby

### Leadership

- Controlled via database lease

### Lease Requirements

Lease stored in PostgreSQL with these fields:

- `owner_id`
- `expiration_timestamp`
- `fencing_token` (monotonic)

### Behavior

Active worker:

- Acquires and renews lease
- Polls actionable rows
- Advances resource state
- Performs external side effects
- Writes updates to database

Standby workers:

- Monitor lease expiration
- Attempt acquisition on expiration
- Become active on successful acquisition

## 5. Database Layer

### System

- PostgreSQL

### Topology

- 3 nodes (1 per provider)

### Role

- Source of truth
- Serialization point for all state transitions
- Worker leadership authority
- Idempotency enforcement

### Replication

- Streaming replication across nodes

### Commit Policy (Production)

- Synchronous replication
- Require at least 1 standby acknowledgment before commit

### Failover

- Managed by HA system, for example Patroni
- Automatic primary promotion on failure

## 6. Availability Model

### Target Guarantees

- System remains operational with loss of any 1 provider

### Conditions

| Providers Available | Behavior                                                        |
| ------------------- | --------------------------------------------------------------- |
| 3                   | Normal operation                                                |
| 2                   | Full operation (writes and processing)                          |
| 1                   | Degraded mode (no guaranteed safe writes if replica ack is required) |

### Constraint

- Safe write availability requires at least 2 database nodes

## 7. Resource Processing Model

### Flow

1. API inserts resource row
2. Worker claims and processes resource
3. Worker updates state in database
4. Client polls resource status

### Properties

- Asynchronous
- Idempotent operations required
- State transitions persisted in DB

## 8. TEE Trust Model

### Requirements

All inter-node communication must be:

- Encrypted
- Mutually authenticated

### Mechanism

1. TEE produces attestation
2. Attestation verified against expected code/measurement
3. Verified node receives identity credential, for example a TLS cert
4. All communication uses mutual TLS

### Enforcement

Only verified nodes can:

- Join cluster
- Participate in worker role
- Access internal services

## 9. Deployment Modes

### Bootstrap Mode

- Single provider
- Single PostgreSQL node
- No replica acknowledgment required
- Reduced durability guarantees

### HA Mode (Target)

- 3 providers
- 3 PostgreSQL nodes
- Synchronous replication with at least 1 standby acknowledgment
- Worker failover enabled
- Full redundancy

## 10. Design Constraints

- Single active worker globally
- No application-level consensus
- No multi-writer database usage
- No sharding required
- One node sufficient for total load

## 11. System Goals

- High availability across providers
- Strong durability guarantees after bootstrap
- Minimal application-level coordination complexity
- Deterministic state progression
- Secure execution via TEE attestation
