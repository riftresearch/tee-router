-- Rename the Hyperliquid spot venue provider id from `hyperliquid` to
-- `hyperliquid_spot`. The chain id remains `hyperliquid`; only provider
-- identifiers are rewritten.

UPDATE public.order_execution_steps
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.order_execution_legs
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.order_provider_addresses
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.order_provider_operations
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.router_route_cost_snapshots
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.router_swap_time_averages
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid';

UPDATE public.market_order_quotes
SET provider_id = regexp_replace(
    provider_id,
    '(^|[:|])hyperliquid($|[|])',
    '\1hyperliquid_spot\2',
    'g'
)
WHERE provider_id ~ '(^|[:|])hyperliquid($|[|])';

UPDATE public.limit_order_quotes
SET provider_id = regexp_replace(
    provider_id,
    '(^|[:|])hyperliquid($|[|])',
    '\1hyperliquid_spot\2',
    'g'
)
WHERE provider_id ~ '(^|[:|])hyperliquid($|[|])';

UPDATE public.router_route_cost_snapshots
SET quote_source = regexp_replace(
    quote_source,
    '(^|[:|])hyperliquid($|[|])',
    '\1hyperliquid_spot\2',
    'g'
)
WHERE quote_source ~ '(^|[:|])hyperliquid($|[|])';

CREATE OR REPLACE FUNCTION public.router_rename_hyperliquid_provider_refs(value jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    updated jsonb := value;
    renamed jsonb;
BEGIN
    IF jsonb_typeof(updated->'transitions') = 'array' THEN
        SELECT jsonb_agg(
            CASE
                WHEN element->>'provider' = 'hyperliquid'
                    THEN jsonb_set(element, '{provider}', to_jsonb('hyperliquid_spot'::text), false)
                ELSE element
            END
            ORDER BY ordinality
        )
        INTO renamed
        FROM jsonb_array_elements(updated->'transitions') WITH ORDINALITY AS item(element, ordinality);

        updated := jsonb_set(updated, '{transitions}', COALESCE(renamed, '[]'::jsonb), false);
    END IF;

    IF jsonb_typeof(updated->'legs') = 'array' THEN
        SELECT jsonb_agg(
            CASE
                WHEN element->>'provider' = 'hyperliquid'
                    THEN jsonb_set(element, '{provider}', to_jsonb('hyperliquid_spot'::text), false)
                ELSE element
            END
            ORDER BY ordinality
        )
        INTO renamed
        FROM jsonb_array_elements(updated->'legs') WITH ORDINALITY AS item(element, ordinality);

        updated := jsonb_set(updated, '{legs}', COALESCE(renamed, '[]'::jsonb), false);
    END IF;

    RETURN updated;
END;
$$;

UPDATE public.market_order_quotes
SET provider_quote = public.router_rename_hyperliquid_provider_refs(provider_quote)
WHERE provider_quote @> '{"transitions":[{"provider":"hyperliquid"}]}'::jsonb
   OR provider_quote @> '{"legs":[{"provider":"hyperliquid"}]}'::jsonb;

UPDATE public.limit_order_quotes
SET provider_quote = public.router_rename_hyperliquid_provider_refs(provider_quote)
WHERE provider_quote @> '{"transitions":[{"provider":"hyperliquid"}]}'::jsonb
   OR provider_quote @> '{"legs":[{"provider":"hyperliquid"}]}'::jsonb;

DROP FUNCTION public.router_rename_hyperliquid_provider_refs(jsonb);

UPDATE public.provider_policies
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid'
  AND NOT EXISTS (
      SELECT 1
      FROM public.provider_policies existing
      WHERE existing.provider = 'hyperliquid_spot'
  );

DELETE FROM public.provider_policies
WHERE provider = 'hyperliquid';

UPDATE public.provider_health_checks
SET provider = 'hyperliquid_spot'
WHERE provider = 'hyperliquid'
  AND NOT EXISTS (
      SELECT 1
      FROM public.provider_health_checks existing
      WHERE existing.provider = 'hyperliquid_spot'
  );

DELETE FROM public.provider_health_checks
WHERE provider = 'hyperliquid';
