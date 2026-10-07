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
import org.springframework.test.web.servlet.MvcResult;

/**
 * HTTP-level checks: duplicate submits are idempotent, conflicts are
 * rejected with a distinguishable reason, and replay is controlled.
 */
@SpringBootTest(classes = Application.class, properties = "events.dispatcher.enabled=false")
@AutoConfigureMockMvc
class EventControllerTest {

    @Autowired
    private MockMvc mvc;

    @Test
    void duplicateSubmitReturnsSameEventAndConflictIsDistinguishable() throws Exception {
        String body = "{\"idempotencyKey\":\"http-1\",\"payload\":\"p1\"}";

        MvcResult first = mvc.perform(post("/api/events")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        String id = com.jayway.jsonpath.JsonPath.read(first.getResponse().getContentAsString(), "$.id");

        // Same key + same payload: idempotent, same id, no extra delivery.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id));

        // Same key + different payload: 409 with distinguishable reason.
        mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"http-1\",\"payload\":\"DIFFERENT\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.reason").value("DUPLICATE_CONFLICT"));

        // Replay of a non-DEAD event is rejected with a distinguishable reason.
        mvc.perform(post("/api/events/{id}/replay", id))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.reason").value("ILLEGAL_STATE"));

        mvc.perform(get("/api/events/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id));
    }
}
