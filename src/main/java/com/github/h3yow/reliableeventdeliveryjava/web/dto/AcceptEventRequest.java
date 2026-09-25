package com.github.h3yow.reliableeventdeliveryjava.web.dto;

/** 接受业务动作请求。 */
public class AcceptEventRequest {
    private String businessKey;
    private String eventType;
    private String payload;

    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
