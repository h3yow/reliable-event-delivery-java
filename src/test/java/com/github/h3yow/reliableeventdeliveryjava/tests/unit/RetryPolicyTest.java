package com.github.h3yow.reliableeventdeliveryjava.tests.unit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.github.h3yow.reliableeventdeliveryjava.domain.RetryPolicy;

class RetryPolicyTest {

    private final RetryPolicy policy =
            new RetryPolicy(5, Duration.ofMillis(100), 2.0, Duration.ofSeconds(10));

    @Test
    void backoffGrowsExponentiallyAndIsCapped() {
        assertThat(policy.backoffAfter(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.backoffAfter(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.backoffAfter(3)).isEqualTo(Duration.ofMillis(400));
        assertThat(policy.backoffAfter(10)).isEqualTo(Duration.ofSeconds(10)); // 上限封顶
    }

    @Test
    void attemptBudgetIsBounded() {
        assertThat(policy.hasRemainingAttempts(0)).isTrue();
        assertThat(policy.hasRemainingAttempts(4)).isTrue();
        assertThat(policy.hasRemainingAttempts(5)).isFalse();
    }
}
