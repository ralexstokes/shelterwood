// Known-bad fixture: raw inline-module names still add one module level.
mod r#nested {
    use super::super::DerivedTreeExport;
}
