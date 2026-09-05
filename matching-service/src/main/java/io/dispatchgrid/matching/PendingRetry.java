package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.RideRequest;

/** A ride parked in the retry store until {@code dueAtMs} (wall clock) for its next pass. */
public record PendingRetry(String cityKey, RideRequest request, int attempt, long dueAtMs) {}
