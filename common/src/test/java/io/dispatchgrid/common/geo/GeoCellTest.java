package io.dispatchgrid.common.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class GeoCellTest {
  static final double LAT = 47.6062;
  static final double LNG = -122.3321;

  @Test
  void nearbyPointsShareACellAndDistantOnesDoNot() {
    String home = GeoCell.cellOf(LAT, LNG, 1000);
    double[] close = Geo.offset(LAT, LNG, 30, 30);
    double[] far = Geo.offset(LAT, LNG, 2500, 0);
    assertThat(GeoCell.cellOf(close[0], close[1], 1000)).isEqualTo(home);
    assertThat(GeoCell.cellOf(far[0], far[1], 1000)).isNotEqualTo(home);
  }

  @Test
  void cellIdIsStableAndRejectsBadSize() {
    assertThat(GeoCell.cellOf(LAT, LNG, 1000)).isEqualTo(GeoCell.cellOf(LAT, LNG, 1000));
    assertThat(GeoCell.cellOf(0, 0, 500)).isEqualTo("0:0");
    assertThatThrownBy(() -> GeoCell.cellOf(LAT, LNG, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
