package me.leaver.rocketmq;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConfigBean {
    @Bean
    public ToolCallbackProvider myTools(MessageService messageService) {
        return MethodToolCallbackProvider
            .builder()
            .toolObjects(messageService)
            .build();
    }
}
