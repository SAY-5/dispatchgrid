package io.dispatchgrid.common.redis;

/** Outcome of an atomic claim attempt. */
public enum ClaimResult {
  /** This ride now owns the driver until the claim expires or is released. */
  CLAIMED,
  /** Another ride holds the driver. */
  TAKEN,
  /** The driver's heartbeat expired; it was dropped from the geo index. */
  STALE
}
