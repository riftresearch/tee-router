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
latest_attempt AS (
    SELECT DISTINCT ON (execution_leg_id)
        execution_leg_id,
        execution_attempt_id
    FROM ranked_actions
    WHERE execution_attempt_id IS NOT NULL
    ORDER BY
        execution_leg_id,
        attempt_index DESC,
        updated_at DESC,
        id DESC
),
action_summary AS (
    SELECT
        execution_leg_id,
        COUNT(*) AS total_actions,
        BOOL_OR(status = 'failed') AS has_failed,
        BOOL_OR(status = 'cancelled') AS has_cancelled,
        BOOL_OR(status = 'running') AS has_running,
        BOOL_OR(status = 'waiting') AS has_waiting,
        BOOL_OR(status = 'ready') AS has_ready,
        COUNT(*) FILTER (WHERE status = 'completed') AS completed_actions,
        COUNT(*) FILTER (WHERE status IN ('completed', 'skipped')) AS terminal_actions,
        MIN(started_at) FILTER (WHERE started_at IS NOT NULL) AS first_started_at,
        MAX(completed_at) FILTER (WHERE completed_at IS NOT NULL) AS last_completed_at
    FROM current_actions
    GROUP BY execution_leg_id
),
first_success_action AS (
    SELECT DISTINCT ON (execution_leg_id)
        execution_leg_id,
        response_json,
        amount_in
    FROM current_actions
    WHERE status IN ('completed', 'skipped')
    ORDER BY execution_leg_id, step_index ASC, updated_at ASC, id ASC
),
last_success_action AS (
    SELECT DISTINCT ON (execution_leg_id)
        execution_leg_id,
        response_json,
        min_amount_out
    FROM current_actions
    WHERE status IN ('completed', 'skipped')
    ORDER BY execution_leg_id, step_index DESC, updated_at DESC, id DESC
),
rolled_up AS (
    SELECT
        action_summary.execution_leg_id,
        CASE
            WHEN action_summary.has_failed THEN 'failed'
            WHEN action_summary.has_cancelled THEN 'cancelled'
            WHEN action_summary.total_actions > 0
                 AND action_summary.completed_actions = action_summary.total_actions
                THEN 'completed'
            WHEN action_summary.total_actions > 0
                 AND action_summary.terminal_actions = action_summary.total_actions
                THEN 'skipped'
            WHEN action_summary.has_running THEN 'running'
            WHEN action_summary.has_waiting THEN 'waiting'
            WHEN action_summary.has_ready THEN 'ready'
            ELSE 'planned'
        END AS status,
        action_summary.total_actions > 0
            AND action_summary.completed_actions = action_summary.total_actions
            AS completed,
        action_summary.first_started_at,
        action_summary.last_completed_at,
        latest_attempt.execution_attempt_id AS latest_execution_attempt_id,
        first_success_action.response_json AS first_response_json,
        first_success_action.amount_in AS first_amount_in,
        last_success_action.response_json AS last_response_json,
        last_success_action.min_amount_out AS last_min_amount_out
    FROM action_summary
    LEFT JOIN latest_attempt
      ON latest_attempt.execution_leg_id = action_summary.execution_leg_id
    LEFT JOIN first_success_action
      ON first_success_action.execution_leg_id = action_summary.execution_leg_id
    LEFT JOIN last_success_action
      ON last_success_action.execution_leg_id = action_summary.execution_leg_id
)
UPDATE public.order_execution_legs leg
SET
    execution_attempt_id = COALESCE(
        rolled_up.latest_execution_attempt_id,
        leg.execution_attempt_id
    ),
    status = rolled_up.status,
    started_at = COALESCE(leg.started_at, rolled_up.first_started_at),
    completed_at = CASE
        WHEN rolled_up.status IN ('completed', 'failed', 'cancelled')
            THEN COALESCE(rolled_up.last_completed_at, leg.completed_at, now())
        ELSE NULL
    END,
    actual_amount_in = CASE
        WHEN rolled_up.completed THEN COALESCE(
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
            rolled_up.first_amount_in,
            rolled_up.first_response_json #>> '{observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,inputAmount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,inputAmount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,source_amount}',
            rolled_up.first_response_json #>> '{observed_state,previous_observed_state,sourceAmount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,amount_in}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,amountIn}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,input_amount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,inputAmount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,source_amount}',
            rolled_up.first_response_json #>> '{observed_state,provider_observed_state,sourceAmount}'
        )
        ELSE NULL
    END,
    actual_amount_out = CASE
        WHEN rolled_up.completed THEN COALESCE(
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
            rolled_up.last_response_json->>'amount_out',
            rolled_up.last_response_json->>'amountOut',
            rolled_up.last_response_json->>'output_amount',
            rolled_up.last_response_json->>'outputAmount',
            rolled_up.last_response_json #>> '{response,amount_out}',
            rolled_up.last_response_json #>> '{response,amountOut}',
            rolled_up.last_response_json #>> '{response,output_amount}',
            rolled_up.last_response_json #>> '{response,outputAmount}',
            rolled_up.last_min_amount_out,
            rolled_up.last_response_json #>> '{observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,destination_amount}',
            rolled_up.last_response_json #>> '{observed_state,previous_observed_state,destinationAmount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,amount_out}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,amountOut}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,output_amount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,outputAmount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,destination_amount}',
            rolled_up.last_response_json #>> '{observed_state,provider_observed_state,destinationAmount}'
        )
        ELSE NULL
    END,
    updated_at = now()
FROM rolled_up
WHERE leg.id = rolled_up.execution_leg_id
  AND leg.leg_type = 'unit_withdrawal';
