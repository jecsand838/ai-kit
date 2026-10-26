/// Parses a user id.
///
/// ```
/// use my_lib::parse_user_id;
/// assert_eq!(parse_user_id("42").unwrap(), 42);
/// ```
///
/// ```compile_fail
/// use my_lib::parse_user_id;
/// let _ = parse_user_id(42);
/// ```
///
/// ```no_run
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let body = reqwest::get("https://example.com").await?.text().await?;
/// println!("{body}");
/// # Ok(())
/// # }
/// ```
pub fn parse_user_id(input: &str) -> Result<u64, ParseUserIdError> {
    input.parse().map_err(ParseUserIdError::from)
}

#[derive(Debug, thiserror::Error)]
#[error("invalid user id")]
pub struct ParseUserIdError(#[from] std::num::ParseIntError);
