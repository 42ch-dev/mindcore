//! Runnable S1–S8 story: memory, adoption, doubt, evidence, shared change, reopen and refusals.
//!
//! Every claim is asserted by `run_story`; a wrong outcome aborts with a non-zero exit code.
//! The scenario and its assertions live in `tests/support/embedded_story.rs`, shared with the
//! `embedded_mind` integration test; this example is a thin runner over that one source.

#[path = "../tests/support/embedded_story.rs"]
mod embedded_story;

#[tokio::main]
async fn main() {
    embedded_story::run_story("example").await;
    println!("story complete: steps 1-8 asserted, exit 0");
}
