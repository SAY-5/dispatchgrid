package io.dispatchgrid.loadgen;

import java.util.List;

/** Simulated service areas. Ids are deliberately 1-based so they map to different shards. */
public record City(int id, String name, double lat, double lng) {
  public static final double EARTH_RADIUS_METERS = 6_371_008.8;

  static final List<City> ALL =
      List.of(
          new City(1, "austin", 30.2672, -97.7431),
          new City(2, "seattle", 47.6062, -122.3321),
          new City(3, "denver", 39.7392, -104.9903),
          new City(4, "chicago", 41.8781, -87.6298));

  public static List<City> first(int n) {
    return ALL.subList(0, Math.min(n, ALL.size()));
  }

  /** Offsets a point by north/east meters. */
  public static double[] offset(double lat, double lng, double northMeters, double eastMeters) {
    double dLat = Math.toDegrees(northMeters / EARTH_RADIUS_METERS);
    double dLng =
        Math.toDegrees(eastMeters / (EARTH_RADIUS_METERS * Math.cos(Math.toRadians(lat))));
    return new double[] {lat + dLat, lng + dLng};
  }
}
