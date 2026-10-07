package com.github.h3yow.reliableeventdeliveryjava.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.h3yow.reliableeventdeliveryjava.Application;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * HTTP surface of the new operations behaviour: capacity rejection is a
 * distinct 429 signal (not a 409 business conflict, not a 500 fault), and
 * the stats endpoint exposes the backlog summary. The dispatcher is switched
 * off here so submitted events stay PENDING for deterministic assertions.
 */
@SpringBootTest(classes = Application.class, properties = {
        "events.dispatcher.enabled=false",
        "events.capacity.max-pending=2"
})
@AutoConfigureMockMvc
@org.springframework.test.annotation.DirtiesContext(
        classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class OpsEndpointsTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void capacityExceededIsDistinctFromConflictAndNotAnInternalError() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-1\",\"payload\":\"p1\"}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-2\",\"payload\":\"p2\"}"))
                .andExpect(status().isOk());

        // Backlog full: new key -> 429 CAPACITY_EXCEEDED.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-3\",\"payload\":\"p3\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.reason").value("CAPACITY_EXCEEDED"));

        // Same key + different payload -> still 409 DUPLICATE_CONFLICT.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-1\",\"payload\":\"OTHER\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_CONFLICT"));

        // Known key + same payload -> still idempotent 200 while full.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-1\",\"payload\":\"p1\"}"))
                .andExpect(status().isOk());
    }

    @Test
    void statsEndpointSummarisesTheBacklog() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"ops-1\",\"payload\":\"p1\"}"))
                .andExpect(status().isOk());

        mvc.perform(get("/api/events/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.counts.PENDING").value(1))
                .andExpect(jsonPath("$.counts.IN_PROGRESS").value(0))
                .andExpect(jsonPath("$.active").value(1))
                .andExpect(jsonPath("$.maxPending").value(2))
                .andExpect(jsonPath("$.accepting").value(true))
                .andExpect(jsonPath("$.oldestPendingAgeMillis").isNumber());
    }
}
