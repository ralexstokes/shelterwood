//! Core library for Shelterwood.

/// Returns a greeting for `name`.
///
/// ```
/// assert_eq!(shelterwood::greet("world"), "hello, world");
/// ```
pub fn greet(name: &str) -> String {
    format!("hello, {name}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn greets_by_name() {
        assert_eq!(greet("shelterwood"), "hello, shelterwood");
    }
}
