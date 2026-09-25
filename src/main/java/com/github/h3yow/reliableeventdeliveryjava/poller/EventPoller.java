package com.github.h3yow.reliableeventdeliveryjava.poller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.github.h3yow.reliableeventdeliveryjava.config.DeliveryProperties;
import com.github.h3yow.reliableeventdeliveryjava.service.EventDeliveryService;

/**
 * 周期性 tick：每次按 worker 数量提交投递任务到有界线程池。
 *
 * <p>单事件处理在独立线程/独立事务内；某事件反复失败、进入退避或终态都不会
 * 阻塞其他事件。线程池大小即可并发的消费者数，并发安全由仓储层条件更新保证。
 */
@Component
public class EventPoller implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(EventPoller.class);

    private final EventDeliveryService deliveryService;
    private final DeliveryProperties properties;

    private ScheduledExecutorService scheduler;
    private ExecutorService workers;
    private final AtomicInteger inFlight = new AtomicInteger();

    public EventPoller(EventDeliveryService deliveryService, DeliveryProperties properties) {
        this.deliveryService = deliveryService;
        this.properties = properties;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (!properties.isPollerEnabled()) {
            log.info("event poller disabled by configuration");
            return;
        }
        start();
    }

    void start() {
        int n = Math.max(1, properties.getWorkerCount());
        workers = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r, "event-worker");
            t.setDaemon(true);
            return t;
        });
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "event-poller");
            t.setDaemon(true);
            return t;
        });
        long periodMs = Math.max(10, properties.getPollInterval().toMillis());
        scheduler.scheduleAtFixedRate(() -> {
            try {
                tick();
            } catch (RuntimeException e) {
                log.error("poller tick failed", e);
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
        log.info("event poller started workerCount={} pollInterval={}ms", n, periodMs);
    }

    void tick() {
        for (int i = 0; i < properties.getWorkerCount(); i++) {
            if (inFlight.get() >= properties.getWorkerCount()) {
                return;
            }
            inFlight.incrementAndGet();
            workers.execute(() -> {
                try {
                    deliveryService.processOne();
                } catch (RuntimeException e) {
                    log.error("event processing failed unexpectedly", e);
                } finally {
                    inFlight.decrementAndGet();
                }
            });
        }
    }

    @Override
    public void destroy() throws InterruptedException {
        log.info("event poller destroying");
        if (scheduler != null) {
            scheduler.shutdownNow();
            if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                log.warn("scheduler did not stop");
            }
        }
        if (workers != null) {
            // 直接强制停止：处理中（可能阻塞在外部副作用上）的线程不阻止关闭；
            // 未写回的事件保持 IN_FLIGHT，由锁超时回收，不丢事件
            workers.shutdownNow();
            if (!workers.awaitTermination(2, TimeUnit.SECONDS)) {
                log.warn("some event-worker threads did not stop within timeout; "
                        + "their in-flight events will be recovered after lock timeout");
            }
        }
        log.info("event poller destroyed");
    }
}
