package io.dispatchgrid.common.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class GeoTest {

  @Test
  void oneDegreeOfLongitudeAtEquatorIsAbout111Km() {
    assertThat(Geo.haversineMeters(0, 0, 0, 1)).isCloseTo(111_195, within(50.0));
  }

  @Test
  void distanceIsSymmetricAndZeroForSamePoint() {
    double ab = Geo.haversineMeters(30.2672, -97.7431, 47.6062, -122.3321);
    double ba = Geo.haversineMeters(47.6062, -122.3321, 30.2672, -97.7431);
    assertThat(ab).isCloseTo(ba, within(1e-6));
    assertThat(ab).isCloseTo(2_838_000, within(15_000.0));
    assertThat(Geo.haversineMeters(10, 10, 10, 10)).isZero();
  }

  @Test
  void offsetRoundTripsThroughDistance() {
    double[] p = Geo.offset(47.6062, -122.3321, 1000, 0);
    assertThat(Geo.haversineMeters(47.6062, -122.3321, p[0], p[1])).isCloseTo(1000, within(1.0));
    double[] q = Geo.offset(47.6062, -122.3321, 0, 1000);
    assertThat(Geo.haversineMeters(47.6062, -122.3321, q[0], q[1])).isCloseTo(1000, within(1.0));
  }
}
