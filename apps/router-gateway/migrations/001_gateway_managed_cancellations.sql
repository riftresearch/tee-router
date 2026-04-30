create table if not exists gateway_managed_cancellations (
  router_order_id uuid primary key,
  auth_mode text not null check (auth_mode in ('gateway_managed_token')),
  auth_policy jsonb not null default '{}'::jsonb,
  encrypted_cancellation_secret text not null,
  gateway_token_hash text not null,
  cancellation_requested_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists gateway_managed_cancellations_gateway_token_hash_idx
  on gateway_managed_cancellations (gateway_token_hash);
