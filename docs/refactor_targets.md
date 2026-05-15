# Refactor Targets

Current branch targets:

- Remove router-level exact-out, minimum-output, maximum-input, and
  execution-tolerance fields from market order quotes/actions.
- Rename router-level quote output to `estimated_amount_out`.
- Keep provider APIs intact, including provider-local request details needed by
  venues.
- Remove manual intervention states and endpoints.
- Use `refund_required` as the admin review queue.
- Remove created-order expiry and use deposit vault `cancel_after` for funding
  watch lifetime.
- Rewrite fresh-db migrations instead of preserving backwards compatibility.
