use crate::{bitcoin::BitcoinChain, evm::EvmChain, traits::ChainOperations};
use router_primitives::ChainType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct ChainRegistry {
    chains: HashMap<ChainType, Arc<dyn ChainOperations>>,
    bitcoin_chains: HashMap<ChainType, Arc<BitcoinChain>>,
    evm_chains: HashMap<ChainType, Arc<EvmChain>>,
}

impl ChainRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
            bitcoin_chains: HashMap::new(),
            evm_chains: HashMap::new(),
        }
    }

    pub fn register(&mut self, chain_type: ChainType, implementation: Arc<dyn ChainOperations>) {
        self.chains.insert(chain_type, implementation);
    }

    pub fn register_bitcoin(&mut self, chain_type: ChainType, implementation: Arc<BitcoinChain>) {
        self.bitcoin_chains
            .insert(chain_type, Arc::clone(&implementation));
        self.chains.insert(chain_type, implementation);
    }

    pub fn register_evm(&mut self, chain_type: ChainType, implementation: Arc<EvmChain>) {
        self.evm_chains
            .insert(chain_type, Arc::clone(&implementation));
        self.chains.insert(chain_type, implementation);
    }

    #[must_use]
    pub fn get(&self, chain_type: &ChainType) -> Option<Arc<dyn ChainOperations>> {
        self.chains.get(chain_type).cloned()
    }

    #[must_use]
    pub fn get_evm(&self, chain_type: &ChainType) -> Option<Arc<EvmChain>> {
        self.evm_chains.get(chain_type).cloned()
    }

    #[must_use]
    pub fn get_bitcoin(&self, chain_type: &ChainType) -> Option<Arc<BitcoinChain>> {
        self.bitcoin_chains.get(chain_type).cloned()
    }

    #[must_use]
    pub fn supported_chains(&self) -> Vec<ChainType> {
        self.chains.keys().copied().collect()
    }
}

impl Default for ChainRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ChainRegistry;
    use crate::evm::EvmChain;
    use router_primitives::ChainType;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn register_evm_exposes_both_generic_and_typed_access() {
        let evm_chain = Arc::new(
            EvmChain::new(
                "http://localhost:8545",
                "0x0000000000000000000000000000000000000001",
                ChainType::Ethereum,
                b"ethereum-wallet",
                4,
                Duration::from_secs(12),
            )
            .await
            .expect("dummy EVM chain should construct"),
        );

        let mut registry = ChainRegistry::new();
        registry.register_evm(ChainType::Ethereum, evm_chain);

        assert!(registry.get(&ChainType::Ethereum).is_some());
        assert!(registry.get_evm(&ChainType::Ethereum).is_some());
    }
}
