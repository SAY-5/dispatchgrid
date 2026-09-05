package io.dispatchgrid.matching;

import io.dispatchgrid.common.geo.RadiusExpansion;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Matching knobs. The claim TTL is the safety net for drivers whose app never reports a completion:
 * a claimed driver returns to the pool when it expires. Retry settings bound how long a ride waits
 * for a driver to free up before it is reported unmatched.
 */
@ConfigurationProperties(prefix = "matching")
public record MatchingProperties(
    int initialRadiusMeters,
    double radiusFactor,
    int maxRadiusMeters,
    int candidatesPerRadius,
    Duration claimTtl,
    Retry retry) {

  /**
   * A ride gets {@code maxAttempts} passes; attempt n waits {@code n * backoff} before the next.
   * The punctuator that replays due rides runs every {@code tick}.
   */
  public record Retry(int maxAttempts, Duration backoff, Duration tick) {
    public Retry {
      if (maxAttempts <= 0) {
        maxAttempts = 3;
      }
      if (backoff == null || backoff.isNegative() || backoff.isZero()) {
        backoff = Duration.ofSeconds(5);
      }
      if (tick == null || tick.isNegative() || tick.isZero()) {
        tick = Duration.ofSeconds(1);
      }
    }
  }

  public MatchingProperties {
    if (initialRadiusMeters <= 0) {
      initialRadiusMeters = 1000;
    }
    if (radiusFactor <= 1.0) {
      radiusFactor = 2.0;
    }
    if (maxRadiusMeters <= 0) {
      maxRadiusMeters = 8000;
    }
    if (candidatesPerRadius <= 0) {
      candidatesPerRadius = 20;
    }
    if (claimTtl == null) {
      claimTtl = Duration.ofSeconds(20);
    }
    if (retry == null) {
      retry = new Retry(0, null, null);
    }
  }

  public RadiusExpansion expansion() {
    return new RadiusExpansion(initialRadiusMeters, radiusFactor, maxRadiusMeters);
  }
}
