WITH candidates AS (
    SELECT
        leg.id,
        leg.actual_amount_in,
        leg.actual_amount_out,
        leg.input_chain_id,
        leg.input_asset_id,
        leg.output_chain_id,
        leg.output_asset_id,
        leg.usd_valuation_json,
        COALESCE(
            first_step.usd_valuation_json #> '{amounts,actualInput}',
            leg.usd_valuation_json #> '{amounts,plannedInput}'
        ) AS input_price,
        COALESCE(
            last_step.usd_valuation_json #> '{amounts,actualOutput}',
            leg.usd_valuation_json #> '{amounts,plannedOutput}',
            leg.usd_valuation_json #> '{amounts,plannedMinOutput}'
        ) AS output_price
    FROM public.order_execution_legs leg
    LEFT JOIN LATERAL (
        SELECT step.usd_valuation_json
        FROM public.order_execution_steps step
        WHERE step.execution_leg_id = leg.id
          AND step.status = 'completed'
        ORDER BY step.step_index ASC, step.completed_at ASC NULLS LAST, step.created_at ASC, step.id ASC
        LIMIT 1
    ) first_step ON true
    LEFT JOIN LATERAL (
        SELECT step.usd_valuation_json
        FROM public.order_execution_steps step
        WHERE step.execution_leg_id = leg.id
          AND step.status = 'completed'
        ORDER BY step.step_index DESC, step.completed_at DESC NULLS LAST, step.created_at DESC, step.id DESC
        LIMIT 1
    ) last_step ON true
    WHERE leg.status = 'completed'
      AND (leg.actual_amount_in IS NOT NULL OR leg.actual_amount_out IS NOT NULL)
),
derived AS (
    SELECT
        id,
        usd_valuation_json,
        CASE
            WHEN actual_amount_in ~ '^[0-9]+$'
             AND usd_valuation_json #> '{amounts,actualInput}' IS NULL
             AND jsonb_typeof(input_price) = 'object'
             AND (input_price->>'unitUsdMicro') ~ '^[0-9]+$'
             AND (input_price->>'decimals') ~ '^[0-9]+$'
             AND (input_price->>'decimals')::int BETWEEN 0 AND 36
                THEN jsonb_strip_nulls(jsonb_build_object(
                    'raw', actual_amount_in,
                    'asset', jsonb_build_object(
                        'chainId', input_chain_id,
                        'assetId', input_asset_id
                    ),
                    'decimals', (input_price->>'decimals')::int,
                    'canonical', input_price->>'canonical',
                    'unitUsdMicro', input_price->>'unitUsdMicro',
                    'amountUsdMicro', floor(
                        (actual_amount_in::numeric * (input_price->>'unitUsdMicro')::numeric)
                        / power(10::numeric, (input_price->>'decimals')::int)
                    )::text
                ))
        END AS actual_input,
        CASE
            WHEN actual_amount_out ~ '^[0-9]+$'
             AND usd_valuation_json #> '{amounts,actualOutput}' IS NULL
             AND jsonb_typeof(output_price) = 'object'
             AND (output_price->>'unitUsdMicro') ~ '^[0-9]+$'
             AND (output_price->>'decimals') ~ '^[0-9]+$'
             AND (output_price->>'decimals')::int BETWEEN 0 AND 36
                THEN jsonb_strip_nulls(jsonb_build_object(
                    'raw', actual_amount_out,
                    'asset', jsonb_build_object(
                        'chainId', output_chain_id,
                        'assetId', output_asset_id
                    ),
                    'decimals', (output_price->>'decimals')::int,
                    'canonical', output_price->>'canonical',
                    'unitUsdMicro', output_price->>'unitUsdMicro',
                    'amountUsdMicro', floor(
                        (actual_amount_out::numeric * (output_price->>'unitUsdMicro')::numeric)
                        / power(10::numeric, (output_price->>'decimals')::int)
                    )::text
                ))
        END AS actual_output
    FROM candidates
),
patched AS (
    SELECT
        id,
        CASE
            WHEN actual_output IS NOT NULL THEN jsonb_set(
                CASE
                    WHEN actual_input IS NOT NULL THEN jsonb_set(
                        base_valuation,
                        '{amounts,actualInput}',
                        actual_input,
                        true
                    )
                    ELSE base_valuation
                END,
                '{amounts,actualOutput}',
                actual_output,
                true
            )
            WHEN actual_input IS NOT NULL THEN jsonb_set(
                base_valuation,
                '{amounts,actualInput}',
                actual_input,
                true
            )
            ELSE base_valuation
        END AS usd_valuation_json
    FROM (
        SELECT
            *,
            jsonb_set(
                usd_valuation_json,
                '{amounts}',
                COALESCE(usd_valuation_json->'amounts', '{}'::jsonb),
                true
            ) AS base_valuation
        FROM derived
    ) prepared
    WHERE actual_input IS NOT NULL
       OR actual_output IS NOT NULL
)
UPDATE public.order_execution_legs leg
SET
    usd_valuation_json = patched.usd_valuation_json,
    updated_at = now()
FROM patched
WHERE leg.id = patched.id;
