package io.dispatchgrid.matching;

import io.dispatchgrid.common.model.RideRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Runs the matcher and, when a pass finds nobody, parks the request in a changelogged state store
 * instead of giving up. A wall-clock punctuator replays due entries, so a ride whose city had no
 * free driver for a few seconds gets matched as soon as one frees up, and a ride is only reported
 * unmatched after the last attempt. The store survives a pod restart through its changelog topic.
 */
public class RetryingMatchProcessor
    implements Processor<String, RideRequest, String, MatchOutcome> {
  static final String STORE = "pending-retries";

  private final MatchService service;
  private final MatchingProperties.Retry retry;
  private ProcessorContext<String, MatchOutcome> ctx;
  private KeyValueStore<String, PendingRetry> store;

  public RetryingMatchProcessor(MatchService service, MatchingProperties.Retry retry) {
    this.service = service;
    this.retry = retry;
  }

  @Override
  public void init(ProcessorContext<String, MatchOutcome> context) {
    this.ctx = context;
    this.store = context.getStateStore(STORE);
    context.schedule(retry.tick(), PunctuationType.WALL_CLOCK_TIME, this::replayDue);
  }

  @Override
  public void process(Record<String, RideRequest> record) {
    attempt(record.key(), record.value(), 1, record.timestamp());
  }

  private void attempt(String cityKey, RideRequest r, int attempt, long timestamp) {
    for (MatchOutcome o : service.handle(r, attempt)) {
      if (o.isRetry()) {
        long due = ctx.currentSystemTimeMs() + backoff(attempt).toMillis();
        store.put(r.rideId(), new PendingRetry(cityKey, r, attempt + 1, due));
      } else {
        ctx.forward(new Record<>(cityKey, o, timestamp));
      }
    }
  }

  private void replayDue(long now) {
    List<PendingRetry> due = new ArrayList<>();
    try (KeyValueIterator<String, PendingRetry> it = store.all()) {
      while (it.hasNext()) {
        PendingRetry p = it.next().value;
        if (p.dueAtMs() <= now) {
          due.add(p);
        }
      }
    }
    for (PendingRetry p : due) {
      store.delete(p.request().rideId());
      attempt(p.cityKey(), p.request(), p.attempt(), now);
    }
  }

  /** Linear backoff: attempt n waits n times the base. */
  Duration backoff(int attempt) {
    return retry.backoff().multipliedBy(attempt);
  }
}
