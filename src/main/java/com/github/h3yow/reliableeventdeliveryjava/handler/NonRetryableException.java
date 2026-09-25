package com.github.h3yow.reliableeventdeliveryjava.handler;

/** 标记一次处理失败为不可重试：事件直接进入 DEAD_LETTER 终态。 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
