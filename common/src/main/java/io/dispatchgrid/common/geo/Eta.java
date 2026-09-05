package io.dispatchgrid.common.geo;

/**
 * Pickup estimate from the straight-line distance recorded at match time. The speed is an effective
 * door-to-door figure (city driving plus the detour a road network adds to a straight line), which
 * is why the default is well below a posted limit.
 */
public final class Eta {
  public static final double DEFAULT_SPEED_MPS = 8.0;

  private Eta() {}

  public static long pickupSeconds(double distanceMeters, double speedMps) {
    double speed = speedMps > 0 ? speedMps : DEFAULT_SPEED_MPS;
    return Math.round(Math.max(0, distanceMeters) / speed);
  }
}
