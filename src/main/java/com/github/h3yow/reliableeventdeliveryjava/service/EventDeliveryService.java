package com.github.h3yow.reliableeventdeliveryjava.service;

/** 认领并投递一条事件（供 poller 的 worker 调用）。 */
public interface EventDeliveryService {

    /** 尝试认领并处理一条事件；返回是否实际处理了事件。 */
    boolean processOne();
}
