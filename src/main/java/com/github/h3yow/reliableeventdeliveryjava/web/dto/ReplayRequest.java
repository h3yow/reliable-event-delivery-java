package com.github.h3yow.reliableeventdeliveryjava.web.dto;

/** 受控重放请求。 */
public class ReplayRequest {
    private boolean force;
    private String reason;

    public boolean isForce() { return force; }
    public void setForce(boolean force) { this.force = force; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
}
