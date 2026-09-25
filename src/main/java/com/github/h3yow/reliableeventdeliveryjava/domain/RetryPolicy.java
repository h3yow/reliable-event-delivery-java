package com.github.h3yow.reliableeventdeliveryjava.domain;

import java.time.Duration;

/**
 * 有上限的重试策略：指数退避 + 抖动，最大尝试次数固定。
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final Duration initialBackoff;
    private final double multiplier;
    private final Duration maxBackoff;

    public RetryPolicy(int maxAttempts, Duration initialBackoff, double multiplier, Duration maxBackoff) {
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.multiplier = multiplier;
        this.maxBackoff = maxBackoff;
    }

    public int getMaxAttempts() { return maxAttempts; }

    /**
     * 计算第 {@code failedAttempt} 次失败后的退避时长（1=第一次失败之后）。
     */
    public Duration backoffAfter(int failedAttempt) {
        long ms = (long) (initialBackoff.toMillis() * Math.pow(multiplier, failedAttempt - 1));
        ms = Math.min(ms, maxBackoff.toMillis());
        return Duration.ofMillis(ms);
    }

    /** 退避后是否还有剩余尝试次数。 */
    public boolean hasRemainingAttempts(int usedAttempts) {
        return usedAttempts < maxAttempts;
    }
}
