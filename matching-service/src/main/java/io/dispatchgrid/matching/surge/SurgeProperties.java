package io.dispatchgrid.matching.surge;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Surge knobs. Demand is the number of ride requests seen in a cell during {@code demandWindow};
 * supply is the number of available drivers whose last position in the cell is younger than {@code
 * supplyTtl}. The multiplier is {@code 1 + slope * (demand / supply - 1)} clamped to {@code [1,
 * maxMultiplier]}, and only kicks in once a cell has at least {@code minDemand} requests so a
 * single ride in an empty cell does not surge.
 */
@ConfigurationProperties(prefix = "surge")
public record SurgeProperties(
    int cellMeters,
    Duration demandWindow,
    Duration supplyTtl,
    double slope,
    double maxMultiplier,
    int minDemand) {

  public SurgeProperties {
    if (cellMeters <= 0) {
      cellMeters = 1000;
    }
    if (demandWindow == null || demandWindow.isZero() || demandWindow.isNegative()) {
      demandWindow = Duration.ofSeconds(60);
    }
    if (supplyTtl == null || supplyTtl.isZero() || supplyTtl.isNegative()) {
      supplyTtl = Duration.ofSeconds(15);
    }
    if (slope <= 0) {
      slope = 0.5;
    }
    if (maxMultiplier < 1.0) {
      maxMultiplier = 3.0;
    }
    if (minDemand <= 0) {
      minDemand = 2;
    }
  }
}
