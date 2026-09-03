package io.dispatchgrid.common.geo;

/** Great-circle helpers on a spherical earth. */
public final class Geo {
  public static final double EARTH_RADIUS_METERS = 6_371_008.8;

  private Geo() {}

  /** Haversine distance in meters between two WGS84 points. */
  public static double haversineMeters(double lat1, double lng1, double lat2, double lng2) {
    double phi1 = Math.toRadians(lat1);
    double phi2 = Math.toRadians(lat2);
    double dPhi = Math.toRadians(lat2 - lat1);
    double dLambda = Math.toRadians(lng2 - lng1);
    double a =
        Math.sin(dPhi / 2) * Math.sin(dPhi / 2)
            + Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) * Math.sin(dLambda / 2);
    return 2 * EARTH_RADIUS_METERS * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  /** Offsets a point by north/east meters. Accurate enough for city-scale simulation. */
  public static double[] offset(double lat, double lng, double northMeters, double eastMeters) {
    double dLat = Math.toDegrees(northMeters / EARTH_RADIUS_METERS);
    double dLng =
        Math.toDegrees(eastMeters / (EARTH_RADIUS_METERS * Math.cos(Math.toRadians(lat))));
    return new double[] {lat + dLat, lng + dLng};
  }
}
