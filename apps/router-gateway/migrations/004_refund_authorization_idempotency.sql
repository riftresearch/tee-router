drop table if exists gateway_refund_authorizations;

create table gateway_refund_authorizations (
  router_order_id uuid primary key,
  refund_mode text not null check (refund_mode in ('evmSignature', 'token')),
  refund_authorizer text,
  encrypted_cancellation_secret text not null,
  cancellation_secret_hash text not null,
  refund_token_hash text,
  encrypted_refund_token text,
  cancellation_requested_at timestamptz,
  cancellation_claimed_at timestamptz,
  cancellation_claim_id uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    (
      refund_mode = 'evmSignature'
      and refund_authorizer is not null
      and refund_token_hash is null
      and encrypted_refund_token is null
    )
    or
    (
      refund_mode = 'token'
      and refund_authorizer is null
      and refund_token_hash is not null
      and encrypted_refund_token is not null
    )
  )
);

create unique index gateway_refund_authorizations_refund_token_hash_idx
  on gateway_refund_authorizations (refund_token_hash)
  where refund_token_hash is not null;

create index gateway_refund_authorizations_active_claim_idx
  on gateway_refund_authorizations (cancellation_claimed_at)
  where cancellation_claimed_at is not null
    and cancellation_requested_at is null;
