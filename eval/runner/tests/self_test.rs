//! The runners' own honesty, gated in CI (#1050, #1051).
//!
//! Both defects #1051 fixed were invisible because nothing exercised them, one of which would have
//! let a broken scorer publish a schema-valid 0/15. The self-tests need no key, nest, model or
//! network, so they are normal tests rather than `--ignored` ones nothing would run.

use nuthatch_eval_runner::{authoring, tier_b, Checks};

/// The result and every named property: a self-test that quietly stopped checking something would
/// otherwise pass, which is the same shape as the defects it guards.
fn assert_exercised(checks: &Checks, probes: &[&str]) {
    let failed: Vec<&String> = checks
        .results
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(n, _)| n)
        .collect();
    assert!(failed.is_empty(), "self-test checks failed: {failed:?}");
    for probe in probes {
        assert!(
            checks.results.iter().any(|(n, _)| n.contains(probe)),
            "the self-test no longer exercises {probe:?}; it cannot pass for the right reason"
        );
    }
}

#[test]
fn the_tier_b_runner_passes_its_own_self_test() {
    assert_exercised(
        &tier_b::self_test(),
        &[
            "unreachable scorer raises",
            "shape mismatch raises (rows is null)",
            "shape mismatch raises (body is not an object)",
            "a well-formed response is returned",
            "evaluate_question propagates a scoring failure",
            "shape mismatch raises (error is null)",
            "a real error string is a rejected query",
            "a rejected query is a diagnosed failure, not an aborted run",
            "a reachable scorer still yields a verdict",
            "report without final_query is refused",
            "report with final_query is accepted",
        ],
    );
}

#[test]
fn the_authoring_runner_passes_its_own_self_test() {
    assert_exercised(
        &authoring::self_test(),
        &[
            "the scenario carries RFC-0017's three criteria",
            "the sealed-through criterion agrees with the chain's finality pin",
            "an unreachable scorer raises",
            "a DECIMAL string equals its number",
            "a wrong total does not pass",
            "a missing column does not pass",
            "the builder skill the subject is given exists",
            "an unindexed nest scores rather than aborting",
            "a nest still sealing is waited for, not scored at zero",
            "wait_for(strict) re-raises rather than reporting None",
            "wait_for without strict still reports None",
            "an unservable nest scores rather than aborting",
            "a run without a sandbox is refused",
            "a operator-asserted report is labelled operator-asserted",
            "a enforced-by-runner report is labelled enforced-by-runner",
            "a pass-through prefix is rejected as a sandbox",
            "an unusable sandbox is rejected, not mistaken for isolation",
            "hiding only the answer key is not isolation",
            "a sandbox with no HTTP client is refused",
            "a confining sandbox is accepted",
            "a malformed /sql shape is fatal (rows is null)",
            "the process inspector sees its own live group",
            "an EPERM zombie-only group is classified safely",
            "an EPERM absent group is classified safely",
            "an EPERM live group is classified safely",
            "an EPERM mixed group is classified safely",
            "an EPERM unknown-state group is classified safely",
            "a failed process inspection does not claim cleanup",
            "a timed-out process inspection does not claim cleanup",
            "a malformed process inspection does not claim cleanup",
            "the reap check has a live child to kill (normal exit)",
            "a backgrounded child is reaped (normal exit)",
            "a backgrounded child is reaped (timeout)",
        ],
    );
}
