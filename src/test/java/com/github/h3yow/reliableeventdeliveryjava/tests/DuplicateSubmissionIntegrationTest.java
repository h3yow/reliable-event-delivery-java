package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.exception.DuplicateEventException;

/**
 * 场景：重复提交同一业务动作。
 * 预期：恰好接受一次、投递一次；重复（含并发重复）被拒绝且原因可区分。
 */
class DuplicateSubmissionIntegrationTest extends IntegrationTestBase {

    @Test
    void duplicateSequentialSubmissionIsRejectedWithDistinguishableReason() {
        String key = "order-" + UUID.randomUUID();

        OutboxEvent first = accept(key, "hello");
        OutboxEvent delivered = awaitStatus(first.getEventId(), EventStatus.DELIVERED);

        assertThatThrownBy(() -> accept(key, "hello-again"))
                .isInstanceOf(DuplicateEventException.class)
                .satisfies(ex -> {
                    DuplicateEventException dup = (DuplicateEventException) ex;
                    assertThat(dup.getReason().name()).isEqualTo("DUPLICATE_BUSINESS_KEY");
                    assertThat(dup.getExistingEventId()).isEqualTo(first.getEventId());
                });

        // 没有产生额外投递
        assertThat(handler.invocationCount(first.getEventId())).isEqualTo(1);
        assertThat(delivered.getStatus()).isEqualTo(EventStatus.DELIVERED);
        printTrail(first.getEventId());
    }

    @Test
    void concurrentDuplicateSubmissionsAcceptExactlyOneEvent() throws Exception {
        String key = "order-concurrent-" + UUID.randomUUID();
        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    accept(key, "race");
                    accepted.incrementAndGet();
                } catch (DuplicateEventException e) {
                    rejected.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(accepted.get()).isEqualTo(1);
        assertThat(rejected.get()).isEqualTo(threads - 1);

        OutboxEvent event = repository.findByBusinessKey(key).orElseThrow();
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);
        assertThat(handler.invocationCount(event.getEventId())).isEqualTo(1);
        printTrail(event.getEventId());
    }
}
