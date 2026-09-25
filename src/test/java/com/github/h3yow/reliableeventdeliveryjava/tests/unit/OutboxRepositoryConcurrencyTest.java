package com.github.h3yow.reliableeventdeliveryjava.tests.unit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;
import com.github.h3yow.reliableeventdeliveryjava.repository.OutboxRepository;
import com.github.h3yow.reliableeventdeliveryjava.tests.IsolatedH2Initializer;

/**
 * 不启动业务处理器：直接压仓储。N 个执行器并发反复认领 K 条事件，
 * 每条事件被认领的“有效归属”在任一时刻只能有一个；总成功认领数恰为 K。
 */
@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(initializers = IsolatedH2Initializer.class)
@TestPropertySource(properties = "reliable.delivery.poller-enabled=false")
class OutboxRepositoryConcurrencyTest {

    @Autowired
    private OutboxRepository repository;

    @Test
    void concurrentClaimsEachEventExactlyOnce() throws Exception {
        int eventCount = 20;
        int claimers = 8;
        List<String> ids = new ArrayList<>();
        Instant now = Instant.now();
        for (int i = 0; i < eventCount; i++) {
            OutboxEvent e = new OutboxEvent();
            e.setEventId(UUID.randomUUID().toString());
            e.setBusinessKey("claim-" + UUID.randomUUID());
            e.setEventType("test.record");
            e.setPayload("ok");
            e.setStatus(EventStatus.PENDING);
            e.setMaxAttempts(5);
            e.setNextAttemptAt(now);
            e.setCreatedAt(now);
            e.setUpdatedAt(now);
            repository.insert(e);
            ids.add(e.getEventId());
        }

        ExecutorService pool = Executors.newFixedThreadPool(claimers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> claimedEventIds = new ConcurrentLinkedQueue<>();

        for (int c = 0; c < claimers; c++) {
            String claimer = "claimer-" + c;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // 每轮都尝试认领，直到所有人都领不到为止
                while (true) {
                    var claimed = repository.claimNext(claimer + "-" + UUID.randomUUID(),
                            Instant.now(), Instant.now().minusSeconds(60));
                    if (claimed.isEmpty()) {
                        return;
                    }
                    claimedEventIds.add(claimed.get().getEventId());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        // 恰好每条事件被成功认领一次
        assertThat(claimedEventIds).hasSize(eventCount);
        assertThat(claimedEventIds).doesNotHaveDuplicates();
        assertThat(claimedEventIds).containsExactlyInAnyOrder(ids.toArray(String[]::new));
        // 全部处于 IN_FLIGHT（被领走，没有完成写回），attempts=1
        for (String id : ids) {
            OutboxEvent e = repository.findByEventId(id).orElseThrow();
            assertThat(e.getStatus()).isEqualTo(EventStatus.IN_FLIGHT);
            assertThat(e.getAttempts()).isEqualTo(1);
        }
    }
}
