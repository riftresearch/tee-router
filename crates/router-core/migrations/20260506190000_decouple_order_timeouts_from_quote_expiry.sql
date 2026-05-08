-- Quote expiry gates only order creation. Once an order exists, historical quote
-- expiry must not drive order expiry or source-vault timeout refunds.

WITH quote_bound_orders AS (
    SELECT
        ro.id AS order_id,
        ro.action_timeout_at AS quote_bound_timeout
    FROM public.router_orders ro
    JOIN public.market_order_quotes moq ON moq.order_id = ro.id
    WHERE ro.action_timeout_at = moq.expires_at
      AND ro.status IN (
          'quoted',
          'pending_funding',
          'funded',
          'executing',
          'refund_required',
          'refunding'
      )

    UNION ALL

    SELECT
        ro.id AS order_id,
        ro.action_timeout_at AS quote_bound_timeout
    FROM public.router_orders ro
    JOIN public.limit_order_quotes loq ON loq.order_id = ro.id
    WHERE ro.action_timeout_at = loq.expires_at
      AND ro.status IN (
          'quoted',
          'pending_funding',
          'funded',
          'executing',
          'refund_required',
          'refunding'
      )
)
UPDATE public.deposit_vaults dv
SET
    cancel_after = GREATEST(
        dv.created_at + INTERVAL '24 hours',
        now() + INTERVAL '24 hours'
    ),
    updated_at = now()
FROM public.custody_vaults cv
JOIN quote_bound_orders qbo ON qbo.order_id = cv.order_id
WHERE cv.id = dv.id
  AND cv.role = 'source_deposit'
  AND dv.status IN ('pending_funding', 'funded')
  AND dv.cancel_after = qbo.quote_bound_timeout;

WITH quote_bound_orders AS (
    SELECT
        ro.id AS order_id,
        ro.action_timeout_at AS quote_bound_timeout
    FROM public.router_orders ro
    JOIN public.market_order_quotes moq ON moq.order_id = ro.id
    WHERE ro.action_timeout_at = moq.expires_at
      AND ro.status IN (
          'quoted',
          'pending_funding',
          'funded',
          'executing',
          'refund_required',
          'refunding'
      )

    UNION ALL

    SELECT
        ro.id AS order_id,
        ro.action_timeout_at AS quote_bound_timeout
    FROM public.router_orders ro
    JOIN public.limit_order_quotes loq ON loq.order_id = ro.id
    WHERE ro.action_timeout_at = loq.expires_at
      AND ro.status IN (
          'quoted',
          'pending_funding',
          'funded',
          'executing',
          'refund_required',
          'refunding'
      )
)
UPDATE public.router_orders ro
SET
    action_timeout_at = CASE
        WHEN ro.order_type = 'limit_order' THEN GREATEST(
            ro.created_at + INTERVAL '3650 days',
            now() + INTERVAL '3650 days'
        )
        ELSE GREATEST(
            ro.created_at + INTERVAL '10 minutes',
            now() + INTERVAL '10 minutes'
        )
    END,
    updated_at = now()
FROM quote_bound_orders qbo
WHERE ro.id = qbo.order_id;
