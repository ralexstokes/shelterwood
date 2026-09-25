use std::future::Future;

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Polls two futures and resolves with the first to become ready.
///
/// Ties are contractual, not incidental: when both are ready in the same
/// poll, `Left` always wins. Callers order a "won" edge before a
/// "closed"/"completed" edge on exactly this bias — a latch that fired and
/// completed must still report the fired side.
pub async fn select_two<A, B>(left: A, right: B) -> Either<A::Output, B::Output>
where
    A: Future + Send,
    B: Future + Send,
{
    tokio::pin!(left);
    tokio::pin!(right);
    tokio::select! {
        biased;
        value = &mut left => Either::Left(value),
        value = &mut right => Either::Right(value),
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn select_two_covers_both_sides_and_biases_ready_ties_left() {
        assert!(matches!(
            super::select_two(std::future::ready(1_u8), std::future::pending::<u8>()).await,
            super::Either::Left(1)
        ));
        assert!(matches!(
            super::select_two(std::future::pending::<u8>(), std::future::ready(2_u8)).await,
            super::Either::Right(2)
        ));
        assert!(matches!(
            super::select_two(std::future::ready(3_u8), std::future::ready(4_u8)).await,
            super::Either::Left(3)
        ));
    }
}
