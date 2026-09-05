package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideUnmatched;

/**
 * Result of one matching pass: exactly one of {@code match} or {@code unmatched} is set. A retry
 * outcome carries the failed pass but is held back by the retry processor instead of emitted.
 */
public record MatchOutcome(Match match, RideUnmatched unmatched, boolean retry) {

  public static MatchOutcome matched(Match m) {
    return new MatchOutcome(m, null, false);
  }

  public static MatchOutcome unmatched(RideUnmatched u) {
    return new MatchOutcome(null, u, false);
  }

  public static MatchOutcome retry(RideUnmatched u) {
    return new MatchOutcome(null, u, true);
  }

  public boolean isMatched() {
    return match != null;
  }

  public boolean isRetry() {
    return retry;
  }
}
