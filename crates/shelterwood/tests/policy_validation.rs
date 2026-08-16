use std::time::Duration;

use shelterwood::{
    Backoff, BackoffFactor, BoundedReadinessDeadline, ExponentialBackoff, FixedBackoff, Intensity,
    Jitter, Mailbox, NonZeroDuration, PolicyError, ReadinessDeadline,
};

#[test]
fn backoff_constructors_reject_every_invalid_boundary() {
    let factor = BackoffFactor::new(2.0).expect("two is a valid factor");

    assert_eq!(
        Backoff::fixed(Duration::ZERO, Jitter::None),
        Err(PolicyError::ZeroDuration)
    );
    assert_eq!(
        Backoff::exponential(Duration::ZERO, factor, Duration::from_secs(1), Jitter::None),
        Err(PolicyError::ZeroDuration)
    );
    assert_eq!(
        Backoff::exponential(Duration::from_secs(1), factor, Duration::ZERO, Jitter::None),
        Err(PolicyError::ZeroDuration)
    );
    assert_eq!(
        Backoff::exponential(
            Duration::from_secs(2),
            factor,
            Duration::from_secs(1),
            Jitter::None,
        ),
        Err(PolicyError::BackoffMaximumBeforeBase)
    );

    for invalid in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(1.0_f64.to_bits() - 1),
    ] {
        assert_eq!(
            BackoffFactor::new(invalid),
            Err(PolicyError::InvalidBackoffFactor)
        );
    }
}

#[test]
fn other_policy_constructors_reject_zero_values() {
    assert_eq!(Mailbox::queue(0), Err(PolicyError::ZeroCapacity));
    assert_eq!(
        ReadinessDeadline::bounded(Duration::ZERO),
        Err(PolicyError::ZeroDuration)
    );
    assert_eq!(
        Intensity::new(1, Duration::ZERO),
        Err(PolicyError::ZeroDuration)
    );
}

/// Every binding here is type-annotated on purpose. The sealing proofs in
/// `policy.rs` are `compile_fail` fences, which pass for *any* compilation
/// error; naming each payload type in a test that must compile keeps them
/// exported and nameable, so the only failure those fences can be resting on
/// is the private-field one they claim.
#[test]
fn sealed_payloads_expose_only_constructor_validated_values() {
    let duration = Duration::from_nanos(1);
    let duration: NonZeroDuration =
        NonZeroDuration::new(duration).expect("the duration is non-zero");
    assert_eq!(duration.get(), Duration::from_nanos(1));

    let fixed_delay = Duration::from_millis(10);
    let Backoff::Fixed(fixed) =
        Backoff::fixed(fixed_delay, Jitter::Equal).expect("the delay is non-zero")
    else {
        panic!("fixed constructor returned another variant");
    };
    let fixed: FixedBackoff = fixed;
    assert_eq!(fixed.delay(), fixed_delay);
    assert_eq!(fixed.jitter(), Jitter::Equal);
    assert!(!fixed.delay().is_zero());

    let base = Duration::from_millis(5);
    let maximum = Duration::from_secs(1);
    let factor = BackoffFactor::new(1.5).expect("the factor is finite and at least one");
    let Backoff::Exponential(exponential) =
        Backoff::exponential(base, factor, maximum, Jitter::None)
            .expect("the exponential bounds are valid")
    else {
        panic!("exponential constructor returned another variant");
    };
    let exponential: ExponentialBackoff = exponential;
    assert_eq!(exponential.base(), base);
    assert_eq!(exponential.factor(), factor);
    assert_eq!(exponential.factor().get(), 1.5);
    assert_eq!(exponential.max(), maximum);
    assert_eq!(exponential.jitter(), Jitter::None);
    assert!(!exponential.base().is_zero());
    assert!(exponential.max() >= exponential.base());

    let deadline = Duration::from_millis(20);
    let ReadinessDeadline::Bounded(bounded) =
        ReadinessDeadline::bounded(deadline).expect("the deadline is non-zero")
    else {
        panic!("bounded constructor returned another variant");
    };
    let bounded: BoundedReadinessDeadline = bounded;
    assert_eq!(bounded.duration(), deadline);
    assert!(!bounded.duration().is_zero());

    let intensity =
        Intensity::new(3, Duration::from_secs(30)).expect("the intensity window is non-zero");
    assert_eq!(intensity.max_restarts(), 3);
    assert_eq!(intensity.within(), Duration::from_secs(30));
    assert!(!intensity.within().is_zero());
}
