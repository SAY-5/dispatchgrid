package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.Match;
import io.dispatchgrid.common.model.RideUnmatched;

/** Result of one matching pass: exactly one of {@code match} or {@code unmatched} is set. */
public record MatchOutcome(Match match, RideUnmatched unmatched) {

  public static MatchOutcome matched(Match m) {
    return new MatchOutcome(m, null);
  }

  public static MatchOutcome unmatched(RideUnmatched u) {
    return new MatchOutcome(null, u);
  }

  public boolean isMatched() {
    return match != null;
  }
}
