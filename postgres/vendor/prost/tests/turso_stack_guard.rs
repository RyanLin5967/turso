#![cfg(any(not(feature = "no-recursion-limit"), feature = "turso-stack-guard"))]

use prost::encoding::{skip_field, DecodeContext, WireType};

#[test]
fn recursive_unknown_groups_keep_the_selected_depth_authority() {
    let result = std::thread::Builder::new()
        .stack_size(512 << 10)
        .spawn(|| {
            let mut bytes = vec![0x0b; 2000];
            bytes.extend(std::iter::repeat(0x0c).take(2001));
            skip_field(
                WireType::StartGroup,
                1,
                &mut bytes.as_slice(),
                DecodeContext::default(),
            )
        })
        .unwrap()
        .join()
        .unwrap();
    let error = result.unwrap_err().to_string();
    #[cfg(not(feature = "no-recursion-limit"))]
    assert!(error.contains("recursion limit reached"), "{error}");
    #[cfg(all(feature = "no-recursion-limit", feature = "turso-stack-guard"))]
    assert!(error.contains("stack depth limit exceeded"), "{error}");
}
