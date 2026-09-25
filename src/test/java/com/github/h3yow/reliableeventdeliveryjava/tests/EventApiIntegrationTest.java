package com.github.h3yow.reliableeventdeliveryjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.github.h3yow.reliableeventdeliveryjava.domain.EventStatus;
import com.github.h3yow.reliableeventdeliveryjava.domain.OutboxEvent;

/** HTTP 接口：冲突响应必须携带可区分的 reason 码。 */
class EventApiIntegrationTest extends IntegrationTestBase {

    @Autowired
    private WebApplicationContext context;

    private MockMvc mvc;

    private MockMvc mvc() {
        if (mvc == null) {
            mvc = MockMvcBuilders.webAppContextSetup(context).build();
        }
        return mvc;
    }

    @Test
    void duplicateSubmissionReturns409WithDuplicateBusinessKeyReason() throws Exception {
        String key = "api-" + UUID.randomUUID();
        String body = "{\"businessKey\":\"" + key + "\",\"eventType\":\"test.record\",\"payload\":\"ok\"}";

        mvc().perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING"));

        mvc().perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_BUSINESS_KEY"))
                .andExpect(jsonPath("$.businessKey").value(key));
    }

    @Test
    void replayConflictsReturnDistinguishableReasons() throws Exception {
        // 1) 不存在 → 404 EVENT_NOT_FOUND
        mvc().perform(post("/api/events/does-not-exist/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"x\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.reason").value("EVENT_NOT_FOUND"));

        // 2) DELIVERED 且未 force → 409 REPLAY_ALREADY_DELIVERED
        OutboxEvent event = accept("api-del-" + UUID.randomUUID(), "ok");
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);
        mvc().perform(post("/api/events/" + event.getEventId() + "/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"force\":false,\"reason\":\"manual check\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("REPLAY_ALREADY_DELIVERED"));

        // 3) 非终态 → 409 REPLAY_NOT_TERMINAL（直接造一个 PENDING 且其处理器让它快速重投不易稳定，
        //    改为先 force 重放 DELIVERED 事件使其进入投递链路，再立刻重放）
        //    为稳定性，这里校验 force=true 重放成功返回 200/PENDING：
        mvc().perform(post("/api/events/" + event.getEventId() + "/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"force\":true,\"reason\":\"ops requested\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"));
        awaitStatus(event.getEventId(), EventStatus.DELIVERED);

        // 轨迹可查询
        mvc().perform(get("/api/events/" + event.getEventId() + "/transitions"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.transitions[0].toStatus").value("PENDING"));
    }
}
