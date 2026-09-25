package com.github.h3yow.reliableeventdeliveryjava.exception;

import com.github.h3yow.reliableeventdeliveryjava.domain.ConflictReason;

/**
 * 业务动作重复提交：同 businessKey 的事件已经存在。
 */
public class DuplicateEventException extends RuntimeException {
    private final String businessKey;
    private final String existingEventId;

    public DuplicateEventException(String businessKey, String existingEventId) {
        super("business action already accepted: businessKey=" + businessKey
                + ", existingEventId=" + existingEventId);
        this.businessKey = businessKey;
        this.existingEventId = existingEventId;
    }

    public ConflictReason getReason() { return ConflictReason.DUPLICATE_BUSINESS_KEY; }
    public String getBusinessKey() { return businessKey; }
    public String getExistingEventId() { return existingEventId; }
}
