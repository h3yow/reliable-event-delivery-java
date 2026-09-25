package com.github.h3yow.reliableeventdeliveryjava.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 投递层可调参数（前缀 reliable.delivery）。 */
@ConfigurationProperties(prefix = "reliable.delivery")
public class DeliveryProperties {

    /** 单事件最大尝试次数（含首次）。 */
    private int maxAttempts = 5;
    /** 首次失败后的退避。 */
    private Duration initialBackoff = Duration.ofMillis(500);
    private double backoffMultiplier = 2.0;
    private Duration maxBackoff = Duration.ofSeconds(30);
    /** IN_FLIGHT 锁超过该时长视为 worker 死亡，允许回收。 */
    private Duration lockTimeout = Duration.ofSeconds(60);
    /** 并发 worker 数。 */
    private int workerCount = 2;
    /** 是否启用后台 poller（测试仓储层并发时可关闭）。 */
    private boolean pollerEnabled = true;
    /** poller 调度间隔。 */
    private Duration pollInterval = Duration.ofMillis(200);
    /** 单次认领扫描的候选行窗口。 */
    private int claimBatchSize = 32;

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Duration getInitialBackoff() { return initialBackoff; }
    public void setInitialBackoff(Duration initialBackoff) { this.initialBackoff = initialBackoff; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public Duration getMaxBackoff() { return maxBackoff; }
    public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }
    public Duration getLockTimeout() { return lockTimeout; }
    public void setLockTimeout(Duration lockTimeout) { this.lockTimeout = lockTimeout; }
    public int getWorkerCount() { return workerCount; }
    public void setWorkerCount(int workerCount) { this.workerCount = workerCount; }
    public Duration getPollInterval() { return pollInterval; }
    public void setPollInterval(Duration pollInterval) { this.pollInterval = pollInterval; }
    public boolean isPollerEnabled() { return pollerEnabled; }
    public void setPollerEnabled(boolean pollerEnabled) { this.pollerEnabled = pollerEnabled; }
    public int getClaimBatchSize() { return claimBatchSize; }
    public void setClaimBatchSize(int claimBatchSize) { this.claimBatchSize = claimBatchSize; }
}
