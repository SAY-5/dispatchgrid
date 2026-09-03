package io.dispatchgrid.matching.surge;

/** The price signal the matcher attaches to a match. */
public interface SurgePricing {

  /** No surge anywhere; used by tests and as the fallback when the tracker is disabled. */
  SurgePricing NONE = (cityId, lat, lng) -> 1.0;

  /** Multiplier for the cell containing the point, at least 1.0. */
  double multiplierAt(int cityId, double lat, double lng);
}
