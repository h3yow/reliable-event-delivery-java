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
 * HTTP-level backpressure: overload is signalled as 429 OVERLOADED —
 * distinguishable from 409 DUPLICATE_CONFLICT (business conflict) and from
 * 500 (internal fault) — and the stats endpoint exposes the backlog.
 */
@SpringBootTest(classes = Application.class, properties = {
        "events.capacity.max-pending=2",
        "events.dispatcher.enabled=false"
})
@AutoConfigureMockMvc
class EventControllerBackpressureTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void overloadIs429WithDedicatedReasonAndStatsExplainTheBacklog() throws Exception {
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"bp-1\",\"payload\":\"p\"}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"bp-2\",\"payload\":\"p\"}"))
                .andExpect(status().isOk());

        // Capacity reached: the third submission is backpressured, not lost.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"bp-3\",\"payload\":\"p\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.reason").value("OVERLOADED"));

        // A business conflict on a known key is still 409, not 429.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"bp-1\",\"payload\":\"DIFFERENT\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_CONFLICT"));

        // The ops summary explains where the backlog sits.
        mvc.perform(get("/api/events/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.countsByStatus.PENDING").value(2))
                .andExpect(jsonPath("$.backlog").value(2))
                .andExpect(jsonPath("$.maxPending").value(2))
                .andExpect(jsonPath("$.oldestPendingWaitMs").isNumber());
    }
}
