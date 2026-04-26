# Legacy Integration Test Utilities

This folder preserves the old OTC integration test helpers after `integration-tests` was deleted.

Contents:

- `mod.rs`: shared process orchestration, port allocation, readiness polling, OTC swap status polling, and test fixture setup
- `test_proxy.rs`: TCP reset proxy used to simulate connection drops and socket resets in tests

These files are intentionally kept as reference material only. They still reflect the OTC/RFQ/MM stack and are not wired into the current workspace.
