use super::shared::*;
use super::*;

/// Quote-refresh child workflow.
///
/// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
#[workflow]
#[derive(Default)]
pub struct QuoteRefreshWorkflow;

#[workflow_methods]
impl QuoteRefreshWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: QuoteRefreshWorkflowInput,
    ) -> WorkflowResult<QuoteRefreshWorkflowOutput> {
        let db_activity_options = db_activity_options();
        let refreshed_attempt = ctx
            .start_activity(
                QuoteRefreshActivities::compose_refreshed_quote_attempt,
                ComposeRefreshedQuoteAttemptInput {
                    order_id: input.order_id,
                    stale_attempt_id: input.stale_attempt_id,
                    failed_step_id: input.failed_step_id,
                },
                db_activity_options.clone(),
            )
            .await?;
        if let RefreshedQuoteAttemptOutcome::Untenable { reason, .. } = &refreshed_attempt.outcome {
            return Ok(QuoteRefreshWorkflowOutput {
                outcome: QuoteRefreshWorkflowOutcome::Untenable {
                    reason: reason.clone(),
                },
            });
        }
        let materialized = ctx
            .start_activity(
                QuoteRefreshActivities::materialize_refreshed_attempt,
                MaterializeRefreshedAttemptInput {
                    order_id: input.order_id,
                    refreshed_attempt,
                },
                db_activity_options,
            )
            .await?;
        Ok(QuoteRefreshWorkflowOutput {
            outcome: QuoteRefreshWorkflowOutcome::Refreshed {
                attempt_id: materialized.attempt_id,
                steps: materialized.steps,
            },
        })
    }
}
