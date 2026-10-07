package com.github.h3yow.reliableeventdeliveryjava.event;

import java.time.Instant;
import java.util.Map;

/**
 * Operational snapshot of the delivery pipeline: where the backlog sits and
 * how retries are distributed. Produced by {@link EventStore#summary}.
 *
 * @param countsByStatus      event count per lifecycle status (all statuses present)
 * @param backlog             PENDING + IN_PROGRESS, i.e. what the capacity limit applies to
 * @param maxPending          configured backlog capacity (submissions are rejected at this point)
 * @param oldestPendingWaitMs age of the longest-waiting PENDING event (since it last
 *                            entered PENDING); null when nothing is pending
 * @param retryDistribution   attempts count -> number of events that have used that many
 *                            attempts (only events with attempts &gt; 0), showing where
 *                            retries cluster
 * @param generatedAt         when this snapshot was taken
 */
public record EventStats(Map<EventStatus, Long> countsByStatus,
                         long backlog,
                         int maxPending,
                         Long oldestPendingWaitMs,
                         Map<Integer, Long> retryDistribution,
                         Instant generatedAt) {
}
