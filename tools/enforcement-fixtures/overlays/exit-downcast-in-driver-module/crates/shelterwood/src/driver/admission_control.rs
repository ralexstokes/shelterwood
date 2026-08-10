// Known-bad fixture: nested driver modules remain on the exit path, so
// runtime type recovery here must be flagged like it is in driver.rs.
pub fn route() {
    let _verdict = payload.downcast::<FrameworkVerdict>();
}
