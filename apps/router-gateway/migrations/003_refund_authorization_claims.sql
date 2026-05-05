alter table gateway_refund_authorizations
  add column if not exists cancellation_claimed_at timestamptz,
  add column if not exists cancellation_claim_id uuid;

create index if not exists gateway_refund_authorizations_active_claim_idx
  on gateway_refund_authorizations (cancellation_claimed_at)
  where cancellation_claimed_at is not null
    and cancellation_requested_at is null;
