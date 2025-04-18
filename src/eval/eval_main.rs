use anyhow::Result;
use zumblezay::eval::run_evaluation;

fn main() -> Result<()> {
    let result = run_evaluation()?;
    println!("{}", result);
    Ok(())
}
