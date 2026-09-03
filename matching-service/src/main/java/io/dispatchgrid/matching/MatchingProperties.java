package io.dispatchgrid.matching;

import io.dispatchgrid.common.geo.RadiusExpansion;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Matching knobs. The claim TTL doubles as the simulated trip length: a claimed driver returns to
 * the pool automatically when it expires, which keeps the synthetic fleet cycling under load.
 */
@ConfigurationProperties(prefix = "matching")
public record MatchingProperties(
    int initialRadiusMeters,
    double radiusFactor,
    int maxRadiusMeters,
    int candidatesPerRadius,
    Duration claimTtl) {

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
  }

  public RadiusExpansion expansion() {
    return new RadiusExpansion(initialRadiusMeters, radiusFactor, maxRadiusMeters);
  }
}
