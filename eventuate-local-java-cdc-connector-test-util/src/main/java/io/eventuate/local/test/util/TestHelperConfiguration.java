package io.eventuate.local.test.util;

import io.eventuate.common.database.spring.jdbc.EventuateCommonJdbcOperationsConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(EventuateCommonJdbcOperationsConfiguration.class)
public class TestHelperConfiguration {

    @Bean
    public TestHelper testHelper() {
        return new TestHelper();
    }

}
