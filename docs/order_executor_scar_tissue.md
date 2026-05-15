# Order Executor Notes

This document records the current executor invariants after the exact-in router
refactor.

## Invariants

- Router market orders are exact-in only.
- `estimated_amount_out` is quote information, not a user execution bound.
- Provider-local request detail JSON may contain venue-native execution
  constraints when the adapter needs them.
- Created orders do not expire. Funding vault watches expire via
  `deposit_vaults.cancel_after`.
- Workflows that cannot determine a safe automatic continuation move the order
  to `refund_required`.
- `refund_required` is the admin review queue; `refunding` means the refund
  workflow is actively running.
