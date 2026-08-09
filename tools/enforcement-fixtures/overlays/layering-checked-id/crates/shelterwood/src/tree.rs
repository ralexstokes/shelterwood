// Known-bad fixture: dynamic child-id validation escaping the reservation
// boundary must be flagged by the layering check.
pub fn reserve() {
    checked_id(candidate);
}
