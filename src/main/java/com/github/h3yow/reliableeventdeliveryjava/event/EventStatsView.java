package com.github.h3yow.reliableeventdeliveryjava.event;

import java.util.Map;

/**
 * Operations-facing summary of the delivery backlog: where events are
 * piling up, how long the oldest one has waited, and how retries are
 * distributed.
 *
 * @param counts                 event count per status
 * @param active                 PENDING + IN_PROGRESS (occupies capacity)
 * @param maxPending             configured capacity limit
 * @param accepting              whether new submissions are currently accepted
 * @param oldestPendingAgeMillis age of the oldest waiting PENDING event;
 *                               null when nothing is waiting
 * @param retryDistribution      attempt count -> number of events that have
 *                               used that many attempts (events with 0
 *                               attempts omitted)
 */
public record EventStatsView(
        Map<EventStatus, Long> counts,
        long active,
        long maxPending,
        boolean accepting,
        Long oldestPendingAgeMillis,
        Map<Integer, Long> retryDistribution) {
}
