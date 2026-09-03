package io.dispatchgrid.common.kafka;

/** Topic names. Every topic is keyed by city id so a city's events stay ordered. */
public final class Topics {
  public static final String RIDE_REQUESTS = "ride-requests";
  public static final String DRIVER_POSITIONS = "driver-positions";
  public static final String RIDE_MATCHES = "ride-matches";
  public static final String RIDE_UNMATCHED = "ride-unmatched";
  public static final int PARTITIONS = 6;

  private Topics() {}

  public static String cityKey(int cityId) {
    return Integer.toString(cityId);
  }
}
