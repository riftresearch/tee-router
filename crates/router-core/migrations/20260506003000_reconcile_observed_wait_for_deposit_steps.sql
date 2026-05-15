UPDATE public.order_execution_steps step
SET
    status = 'completed',
    tx_hash = COALESCE(step.tx_hash, vault.funding_tx_hash),
    response_json = CASE
        WHEN step.response_json = '{}'::jsonb THEN jsonb_strip_nulls(jsonb_build_object(
            'reason', 'funding_vault_funded',
            'tx_hash', vault.funding_tx_hash,
            'sender_address', vault.funding_sender_address,
            'sender_addresses', vault.funding_sender_addresses,
            'recipient_address', vault.funding_recipient_address,
            'transfer_index', vault.funding_transfer_index,
            'vout', vault.funding_transfer_index,
            'amount', vault.funding_observed_amount,
            'confirmation_state', vault.funding_confirmation_state,
            'observed_at', vault.funding_observed_at
        ))
        ELSE step.response_json
    END,
    completed_at = COALESCE(step.completed_at, vault.funding_observed_at, now()),
    updated_at = now()
FROM public.router_orders ro
JOIN public.deposit_vaults vault
  ON vault.id = ro.funding_vault_id
WHERE step.order_id = ro.id
  AND step.step_type = 'wait_for_deposit'
  AND step.status = 'waiting'
  AND vault.funding_observed_amount IS NOT NULL;
