drop table if exists gateway_managed_cancellations;

create table if not exists gateway_refund_authorizations (
  router_order_id uuid primary key,
  refund_mode text not null check (refund_mode in ('evmSignature', 'token')),
  refund_authorizer text,
  encrypted_cancellation_secret text not null,
  refund_token_hash text,
  cancellation_requested_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (
    (refund_mode = 'evmSignature' and refund_authorizer is not null and refund_token_hash is null)
    or
    (refund_mode = 'token' and refund_authorizer is null and refund_token_hash is not null)
  )
);

create unique index if not exists gateway_refund_authorizations_refund_token_hash_idx
  on gateway_refund_authorizations (refund_token_hash)
  where refund_token_hash is not null;
