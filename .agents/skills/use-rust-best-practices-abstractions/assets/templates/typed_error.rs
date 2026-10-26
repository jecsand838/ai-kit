#[derive(Debug, thiserror::Error)]
pub enum ParseAmountError {
    #[error("amount must be positive")]
    NonPositive,
    #[error("invalid number: {0}")]
    InvalidNumber(#[from] std::num::ParseFloatError),
}

pub fn parse_amount(input: &str) -> Result<f64, ParseAmountError> {
    let value: f64 = input.parse()?;
    if value <= 0.0 {
        return Err(ParseAmountError::NonPositive);
    }
    Ok(value)
}

pub fn run_cli(input: &str) -> anyhow::Result<()> {
    let amount = parse_amount(input)
        .map_err(|error| anyhow::anyhow!("failed to parse amount from CLI input: {error}"))?;
    println!("{amount}");
    Ok(())
}
