WITH ranked_actions AS (
    SELECT
        steps.*,
        COALESCE(attempts.attempt_index, 0) AS attempt_index,
        ROW_NUMBER() OVER (
            PARTITION BY steps.execution_leg_id, steps.step_index
            ORDER BY
                COALESCE(attempts.attempt_index, 0) DESC,
                steps.updated_at DESC,
                steps.id DESC
        ) AS action_rank
    FROM public.order_execution_steps steps
    LEFT JOIN public.order_execution_attempts attempts
      ON attempts.id = steps.execution_attempt_id
    WHERE steps.execution_leg_id IS NOT NULL
),
current_actions AS (
    SELECT *
    FROM ranked_actions
    WHERE action_rank = 1
),
action_summary AS (
    SELECT
        execution_leg_id,
        COUNT(*) AS total_actions,
        COUNT(*) FILTER (WHERE status = 'completed') AS completed_actions,
        MIN(started_at) FILTER (WHERE started_at IS NOT NULL) AS first_started_at,
        MAX(completed_at) FILTER (WHERE completed_at IS NOT NULL) AS last_completed_at
    FROM current_actions
    GROUP BY execution_leg_id
),
first_success_action AS (
    SELECT DISTINCT ON (execution_leg_id)
        execution_leg_id,
        response_json,
        request_json,
        amount_in
    FROM current_actions
    WHERE status = 'completed'
    ORDER BY execution_leg_id, step_index ASC, updated_at ASC, id ASC
),
last_success_action AS (
    SELECT DISTINCT ON (execution_leg_id)
        execution_leg_id,
        response_json,
        request_json,
        amount_in,
        min_amount_out
    FROM current_actions
    WHERE status = 'completed'
    ORDER BY execution_leg_id, step_index DESC, updated_at DESC, id DESC
),
rolled_up AS (
    SELECT
        action_summary.execution_leg_id,
        action_summary.first_started_at,
        action_summary.last_completed_at,
        first_success_action.response_json AS first_response_json,
        first_success_action.request_json AS first_request_json,
        first_success_action.amount_in AS first_amount_in,
        last_success_action.response_json AS last_response_json,
        last_success_action.request_json AS last_request_json,
        last_success_action.amount_in AS last_amount_in,
        last_success_action.min_amount_out AS last_min_amount_out
    FROM action_summary
    JOIN first_success_action
      ON first_success_action.execution_leg_id = action_summary.execution_leg_id
    JOIN last_success_action
      ON last_success_action.execution_leg_id = action_summary.execution_leg_id
    WHERE action_summary.total_actions > 0
      AND action_summary.completed_actions = action_summary.total_actions
),
amounts AS (
    SELECT
        leg.id AS execution_leg_id,
        COALESCE(
            (
                SELECT probe.value->>'debit_delta'
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(rolled_up.first_response_json #> '{balance_observation,probes}') = 'array'
                            THEN rolled_up.first_response_json #> '{balance_observation,probes}'
                        ELSE '[]'::jsonb
                    END
                ) AS probe(value)
                WHERE probe.value->>'role' = 'source'
                  AND probe.value->>'debit_delta' ~ '^[0-9]+$'
                  AND (probe.value->>'debit_delta')::numeric > 0
                LIMIT 1
            ),
            rolled_up.first_response_json->>'amount_in',
            rolled_up.first_response_json->>'amountIn',
            rolled_up.first_response_json->>'input_amount',
            rolled_up.first_response_json->>'inputAmount',
            rolled_up.first_response_json #>> '{response,amount_in}',
            rolled_up.first_response_json #>> '{response,amountIn}',
            rolled_up.first_response_json #>> '{response,input_amount}',
            rolled_up.first_response_json #>> '{response,inputAmount}',
            rolled_up.first_response_json #>> '{response,inputAmount}',
            rolled_up.first_response_json #>> '{provider_context,amount_in}',
            rolled_up.first_response_json #>> '{provider_context,amountIn}',
            rolled_up.first_response_json #>> '{provider_context,input_amount}',
            rolled_up.first_response_json #>> '{provider_context,inputAmount}',
            rolled_up.first_response_json #>> '{provider_context,amount}',
            rolled_up.first_request_json->>'amount_in',
            rolled_up.first_request_json->>'amountIn',
            rolled_up.first_request_json->>'input_amount',
            rolled_up.first_request_json->>'inputAmount',
            rolled_up.first_request_json->>'amount',
            rolled_up.first_response_json #>> '{observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,inputAmount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,inputAmount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,inputAmount}',
            rolled_up.first_amount_in
        ) AS actual_amount_in,
        COALESCE(
            (
                SELECT probe.value->>'credit_delta'
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(rolled_up.last_response_json #> '{balance_observation,probes}') = 'array'
                            THEN rolled_up.last_response_json #> '{balance_observation,probes}'
                        ELSE '[]'::jsonb
                    END
                ) AS probe(value)
                WHERE probe.value->>'role' = 'destination'
                  AND probe.value->>'credit_delta' ~ '^[0-9]+$'
                  AND (probe.value->>'credit_delta')::numeric > 0
                LIMIT 1
            ),
            CASE
                WHEN leg.leg_type = 'cctp_bridge' THEN (
                    SELECT probe.value->>'credit_delta'
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(rolled_up.last_response_json #> '{balance_observation,probes}') = 'array'
                                THEN rolled_up.last_response_json #> '{balance_observation,probes}'
                            ELSE '[]'::jsonb
                        END
                    ) AS probe(value)
                    WHERE probe.value->>'role' = 'source'
                      AND probe.value->>'credit_delta' ~ '^[0-9]+$'
                      AND (probe.value->>'credit_delta')::numeric > 0
                    LIMIT 1
                )
            END,
            rolled_up.last_response_json->>'amount_out',
            rolled_up.last_response_json->>'amountOut',
            rolled_up.last_response_json->>'output_amount',
            rolled_up.last_response_json->>'outputAmount',
            rolled_up.last_response_json->>'expectedOutputAmount',
            rolled_up.last_response_json->>'minOutputAmount',
            rolled_up.last_response_json #>> '{response,amount_out}',
            rolled_up.last_response_json #>> '{response,amountOut}',
            rolled_up.last_response_json #>> '{response,output_amount}',
            rolled_up.last_response_json #>> '{response,outputAmount}',
            rolled_up.last_response_json #>> '{response,expectedOutputAmount}',
            rolled_up.last_response_json #>> '{response,minOutputAmount}',
            rolled_up.last_response_json #>> '{provider_context,amount_out}',
            rolled_up.last_response_json #>> '{provider_context,amountOut}',
            rolled_up.last_response_json #>> '{provider_context,output_amount}',
            rolled_up.last_response_json #>> '{provider_context,outputAmount}',
            rolled_up.last_request_json->>'amount_out',
            rolled_up.last_request_json->>'amountOut',
            rolled_up.last_request_json->>'output_amount',
            rolled_up.last_request_json->>'outputAmount',
            CASE
                WHEN leg.leg_type = 'cctp_bridge'
                    THEN rolled_up.last_request_json->>'amount'
            END,
            rolled_up.last_request_json #>> '{price_route,destAmount}',
            rolled_up.last_min_amount_out,
            rolled_up.last_response_json #>> '{observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,previous_observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,previous_observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,decoded_message_body,amount}',
            CASE
                WHEN leg.leg_type = 'cctp_bridge'
                    THEN rolled_up.last_amount_in
            END
        ) AS actual_amount_out,
        rolled_up.first_started_at,
        rolled_up.last_completed_at
    FROM rolled_up
    JOIN public.order_execution_legs leg
      ON leg.id = rolled_up.execution_leg_id
)
UPDATE public.order_execution_legs leg
SET
    status = 'completed',
    started_at = COALESCE(leg.started_at, amounts.first_started_at),
    completed_at = COALESCE(amounts.last_completed_at, leg.completed_at, now()),
    actual_amount_in = amounts.actual_amount_in,
    actual_amount_out = amounts.actual_amount_out,
    updated_at = now()
FROM amounts
WHERE leg.id = amounts.execution_leg_id
  AND amounts.actual_amount_in ~ '^0*[1-9][0-9]*$'
  AND amounts.actual_amount_out ~ '^0*[1-9][0-9]*$'
  AND (
      leg.status <> 'completed'
      OR leg.actual_amount_in IS NULL
      OR leg.actual_amount_out IS NULL
  );
