package io.dispatchgrid.common.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class RadiusExpansionTest {

  @Test
  void doublesUntilCapAndEndsExactlyAtCap() {
    assertThat(new RadiusExpansion(500, 2.0, 5000).radii())
        .containsExactly(500, 1000, 2000, 4000, 5000);
  }

  @Test
  void capEqualToInitialGivesSingleAttempt() {
    assertThat(new RadiusExpansion(1500, 2.0, 1500).radii()).containsExactly(1500);
  }

  @Test
  void capOnPowerOfFactorIsNotDuplicated() {
    assertThat(new RadiusExpansion(1000, 2.0, 4000).radii()).containsExactly(1000, 2000, 4000);
  }

  @Test
  void rejectsInvalidArguments() {
    assertThatThrownBy(() -> new RadiusExpansion(0, 2.0, 100))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RadiusExpansion(100, 1.0, 100))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RadiusExpansion(100, 2.0, 50))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
