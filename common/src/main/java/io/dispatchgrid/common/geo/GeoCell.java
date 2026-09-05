package io.dispatchgrid.common.geo;

/**
 * Fixed-size grid cells over WGS84 coordinates. A cell id is "row:col" where row and col are the
 * integer counts of {@code cellMeters} north of the equator and east of the meridian at the point's
 * latitude, so two points fewer than {@code cellMeters} apart share a cell most of the time and
 * never sit more than two cells apart.
 */
public final class GeoCell {
  private static final double METERS_PER_DEGREE_LAT = 111_320.0;

  private GeoCell() {}

  public static String cellOf(double lat, double lng, int cellMeters) {
    if (cellMeters <= 0) {
      throw new IllegalArgumentException("cellMeters must be positive");
    }
    long row = (long) Math.floor(lat * METERS_PER_DEGREE_LAT / cellMeters);
    double metersPerDegreeLng =
        METERS_PER_DEGREE_LAT * Math.max(Math.cos(Math.toRadians(lat)), 1e-6);
    long col = (long) Math.floor(lng * metersPerDegreeLng / cellMeters);
    return row + ":" + col;
  }
}
