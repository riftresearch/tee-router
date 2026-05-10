use super::shared::*;
use super::*;

/// Provider-hint fallback polling child workflow.
///
/// Scar tissue: §10 provider operation hint flow. This workflow is the polling half of the
/// signal-first plus fallback shape for brief §8.4.
#[workflow]
#[derive(Default)]
pub struct ProviderHintPollWorkflow;

#[workflow_methods]
impl ProviderHintPollWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: ProviderHintPollWorkflowInput,
    ) -> WorkflowResult<ProviderHintPollWorkflowOutput> {
        let db_activity_options = db_activity_options();
        let mut elapsed = Duration::ZERO;

        loop {
            let polled = ctx
                .start_activity(
                    ProviderObservationActivities::poll_provider_operation_hints,
                    PollProviderOperationHintsInput {
                        order_id: input.order_id,
                        step_id: input.step_id,
                    },
                    db_activity_options.clone(),
                )
                .await?;
            match polled.decision {
                ProviderOperationHintDecision::Accept | ProviderOperationHintDecision::Reject => {
                    return Ok(ProviderHintPollWorkflowOutput {
                        provider_operation_id: polled.provider_operation_id,
                        decision: polled.decision,
                        reason: polled.reason,
                    });
                }
                ProviderOperationHintDecision::Defer => {
                    if elapsed >= PROVIDER_HINT_WAIT_TIMEOUT {
                        return Ok(ProviderHintPollWorkflowOutput {
                            provider_operation_id: polled.provider_operation_id,
                            decision: ProviderOperationHintDecision::Defer,
                            reason: Some(
                                polled.reason.unwrap_or_else(|| {
                                    "provider hint polling timed out".to_string()
                                }),
                            ),
                        });
                    }
                    ctx.timer(PROVIDER_HINT_POLL_INTERVAL).await;
                    elapsed = elapsed.saturating_add(PROVIDER_HINT_POLL_INTERVAL);
                }
            }
        }
    }
}
