package com.github.h3yow.reliableeventdeliveryjava.tests;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * 让每个测试上下文使用独立命名的 H2 内存库，
 * 杜绝跨上下文（@DirtiesContext 重建之间）共享数据与事件串扰。
 */
public class IsolatedH2Initializer
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        String dbName = "red-test-" + java.util.UUID.randomUUID();
        TestPropertyValues.of(
                "spring.datasource.url=jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1"
        ).applyTo(context.getEnvironment());
    }
}
