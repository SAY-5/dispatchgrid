package io.dispatchgrid.common.geo;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EtaTest {
  @Test
  void dividesDistanceBySpeedAndRounds() {
    assertThat(Eta.pickupSeconds(800, 8)).isEqualTo(100);
    assertThat(Eta.pickupSeconds(1234, 8)).isEqualTo(154);
    assertThat(Eta.pickupSeconds(0, 8)).isZero();
  }

  @Test
  void fallsBackToTheDefaultSpeedWhenTheSettingIsUnusable() {
    assertThat(Eta.pickupSeconds(800, 0)).isEqualTo(100);
    assertThat(Eta.pickupSeconds(800, -3)).isEqualTo(100);
  }
}
