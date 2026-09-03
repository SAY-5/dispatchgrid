package io.dispatchgrid.common.geo;

import java.util.ArrayList;
import java.util.List;

/**
 * The sequence of search radii the matcher tries before giving up: start at {@code initialMeters},
 * multiply by {@code factor}, and always finish exactly at {@code maxMeters}.
 */
public record RadiusExpansion(int initialMeters, double factor, int maxMeters) {

  public RadiusExpansion {
    if (initialMeters <= 0) {
      throw new IllegalArgumentException("initialMeters must be positive");
    }
    if (factor <= 1.0) {
      throw new IllegalArgumentException("factor must be greater than 1");
    }
    if (maxMeters < initialMeters) {
      throw new IllegalArgumentException("maxMeters must be at least initialMeters");
    }
  }

  public List<Integer> radii() {
    List<Integer> out = new ArrayList<>();
    double r = initialMeters;
    while (r < maxMeters) {
      out.add((int) Math.round(r));
      r *= factor;
    }
    out.add(maxMeters);
    return out;
  }
}
