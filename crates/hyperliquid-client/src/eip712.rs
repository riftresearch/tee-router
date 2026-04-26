//! Minimal EIP-712 glue. Each signable action implements this trait by
//! declaring its own domain + `struct_hash`; the signing hash is derived
//! automatically per EIP-712 § encode() (0x19, 0x01, domainSeparator, structHash).

use alloy::{
    dyn_abi::Eip712Domain,
    primitives::{keccak256, B256},
};

pub trait Eip712 {
    fn domain(&self) -> Eip712Domain;
    fn struct_hash(&self) -> B256;

    fn eip712_signing_hash(&self) -> B256 {
        let mut digest_input = [0u8; 2 + 32 + 32];
        digest_input[0] = 0x19;
        digest_input[1] = 0x01;
        digest_input[2..34].copy_from_slice(self.domain().hash_struct().as_slice());
        digest_input[34..66].copy_from_slice(self.struct_hash().as_slice());
        keccak256(digest_input)
    }
}
