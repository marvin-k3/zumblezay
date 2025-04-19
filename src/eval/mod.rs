use anyhow::Result;

mod app;

pub use app::main;

/// Example function that can be called from eval_main.rs
pub fn run_evaluation() -> Result<String> {
    Ok("Evaluation completed successfully".to_string())
}
