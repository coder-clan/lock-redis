package org.coderclan.lock.redis;

import org.coderclan.lock.LockService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by aray.chou.cn(at)gmail(dot)com on 8/15/2018.
 */
@Configuration
public class LockRedisAutoConfiguration {

    @ConditionalOnMissingBean(LockService.class)
    @Bean
    LockService idempotentValidator() {
        LockServiceRedisImpl bean = new LockServiceRedisImpl();
        return bean;
    }
}
