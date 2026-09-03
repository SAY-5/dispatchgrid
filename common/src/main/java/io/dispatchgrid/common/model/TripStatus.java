package io.dispatchgrid.common.model;

/** Lifecycle of a trip row in the city shard. */
public enum TripStatus {
  REQUESTED,
  MATCHED,
  UNMATCHED,
  COMPLETED,
  CANCELLED
}
