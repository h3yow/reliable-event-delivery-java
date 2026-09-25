package com.github.h3yow.reliableeventdeliveryjava.tests;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.github.h3yow.reliableeventdeliveryjava.handler.EventHandler;
import com.github.h3yow.reliableeventdeliveryjava.handler.NonRetryableException;

/**
 * 测试用可控处理器：记录每个 eventId 的调用次数，可按 payload 指令失败。
 * 不自行去重——去重/并发互斥由投递层保证；计数用于断言“无重复副作用”。
 */
@Component
public class RecordingHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(RecordingHandler.class);
    public static final String TYPE = "test.record";

    private final Map<String, AtomicInteger> invocationCountByEvent = new ConcurrentHashMap<>();
    private final List<String> deliveredOrder = new CopyOnWriteArrayList<>();
    private volatile CountDownLatch blocker = new CountDownLatch(0);

    /** 让 BLOCK 负载的处理在门闩释放前一直阻塞（事件稳定停留在 IN_FLIGHT）。 */
    public void blockUntil(CountDownLatch latch) {
        this.blocker = latch;
    }

    @Override
    public String supportsType() {
        return TYPE;
    }

    @Override
    public void handle(String eventId, String payload) throws InterruptedException {
        int count = invocationCountByEvent.computeIfAbsent(eventId, k -> new AtomicInteger()).incrementAndGet();
        log.info("RecordingHandler invoked eventId={} invocationNo={} payload={}", eventId, count, payload);
        deliveredOrder.add(eventId);
        if ("BLOCK".equals(payload)) {
            try {
                blocker.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            return;
        }
        if ("BLOCK_FIRST_ONLY".equals(payload)) {
            // 第一次调用在门闩上悬挂（模拟处理中途崩溃）；后续（回收/重放）调用直接成功。
            // 响应中断：上下文销毁时 worker 线程能立即退出，不留下僵尸消费者。
            if (count == 1) {
                try {
                    blocker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
            return;
        }
        if ("FAIL_ALWAYS".equals(payload)) {
            throw new RuntimeException("simulated transient failure for " + eventId);
        }
        if ("FAIL_NON_RETRYABLE".equals(payload)) {
            throw new NonRetryableException("simulated permanent failure for " + eventId);
        }
        // 默认成功
    }

    public int invocationCount(String eventId) {
        AtomicInteger c = invocationCountByEvent.get(eventId);
        return c == null ? 0 : c.get();
    }

    public int totalInvocations() {
        return invocationCountByEvent.values().stream().mapToInt(AtomicInteger::get).sum();
    }

    public List<String> deliveredOrder() {
        return List.copyOf(deliveredOrder);
    }

    public void reset() {
        blocker.countDown(); // 先释放可能悬挂在上一门闩上的线程
        invocationCountByEvent.clear();
        deliveredOrder.clear();
        this.blocker = new CountDownLatch(0);
    }
}
