use router_core::error::RouterCoreError;
use snafu::Snafu;
use temporalio_sdk::activities::ActivityError;

use super::types::WorkflowStepId;

#[derive(Debug, Snafu)]
pub enum OrderActivityError {
    #[snafu(display("database query failed"))]
    DbQuery { source: RouterCoreError },

    #[snafu(display("provider quote failed for {provider}: {source}"))]
    ProviderQuote {
        provider: String,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("provider execution failed for {provider}: {source}"))]
    ProviderExecute {
        provider: String,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("provider observation failed for {provider}: {source}"))]
    ProviderObserve {
        provider: String,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("missing activity dependency: {component}"))]
    MissingConfiguration { component: &'static str },

    #[snafu(display("serialization failed while {context}: {source}"))]
    Serialization {
        context: String,
        source: serde_json::Error,
    },

    #[snafu(display("missing {vault_role} hydration for step {step_id}"))]
    MissingHydration {
        vault_role: &'static str,
        step_id: WorkflowStepId,
    },

    #[snafu(display("order workflow invariant violated ({invariant}): {detail}"))]
    OrdershipInvariant {
        invariant: &'static str,
        detail: String,
    },

    #[snafu(display("amount parse failed while {context}: {source}"))]
    AmountParse {
        context: String,
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("refund materialization failed while {context}"))]
    RefundMaterialization { context: String },

    #[snafu(display("invalid terminal state {current}; expected {expected}"))]
    InvalidTerminalState {
        current: String,
        expected: &'static str,
    },

    #[allow(dead_code)]
    #[snafu(display("{message}"))]
    Whatever { message: String },
}

impl OrderActivityError {
    #[must_use]
    pub fn db_query(source: RouterCoreError) -> Self {
        Self::DbQuery { source }
    }

    #[must_use]
    pub fn missing_configuration(component: &'static str) -> Self {
        Self::MissingConfiguration { component }
    }

    #[must_use]
    pub fn provider_quote(provider: impl Into<String>, source: impl ToString) -> Self {
        Self::ProviderQuote {
            provider: provider.into(),
            source: source.to_string(),
        }
    }

    #[must_use]
    pub fn provider_execute(provider: impl Into<String>, source: impl ToString) -> Self {
        Self::ProviderExecute {
            provider: provider.into(),
            source: source.to_string(),
        }
    }

    #[must_use]
    pub fn provider_observe(provider: impl Into<String>, source: impl ToString) -> Self {
        Self::ProviderObserve {
            provider: provider.into(),
            source: source.to_string(),
        }
    }

    #[must_use]
    pub fn serialization(context: impl Into<String>, source: serde_json::Error) -> Self {
        Self::Serialization {
            context: context.into(),
            source,
        }
    }

    #[must_use]
    pub fn invariant(invariant: &'static str, detail: impl Into<String>) -> Self {
        Self::OrdershipInvariant {
            invariant,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn amount_parse(context: impl Into<String>, source: impl ToString) -> Self {
        Self::AmountParse {
            context: context.into(),
            source: source.to_string(),
        }
    }

    #[must_use]
    pub fn refund_materialization(context: impl Into<String>) -> Self {
        Self::RefundMaterialization {
            context: context.into(),
        }
    }

    #[must_use]
    pub fn invalid_terminal_state(current: impl Into<String>, expected: &'static str) -> Self {
        Self::InvalidTerminalState {
            current: current.into(),
            expected,
        }
    }

    #[must_use]
    pub fn whatever(message: impl ToString) -> Self {
        Self::Whatever {
            message: message.to_string(),
        }
    }

    #[must_use]
    pub fn into_activity_error(self) -> ActivityError {
        self.into()
    }
}

impl From<ActivityError> for OrderActivityError {
    fn from(source: ActivityError) -> Self {
        Self::whatever(format!("{source:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::OrderActivityError;

    #[test]
    fn whatever_preserves_message() {
        let error = OrderActivityError::whatever("legacy message");

        assert_eq!(error.to_string(), "legacy message");
    }

    #[test]
    fn invariant_display_includes_name_and_detail() {
        let error = OrderActivityError::invariant("step_owner", "wrong attempt");
        let rendered = error.to_string();

        assert!(rendered.contains("step_owner"));
        assert!(rendered.contains("wrong attempt"));
    }

    #[test]
    fn provider_execute_display_includes_provider_and_source() {
        let error = OrderActivityError::provider_execute("hyperliquid", "rejected");
        let rendered = error.to_string();

        assert!(rendered.contains("hyperliquid"));
        assert!(rendered.contains("rejected"));
    }
}
