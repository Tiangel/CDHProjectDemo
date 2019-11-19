package com.cloudera.phoenixdemo.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KafkaSaslConfig  {

    @Value("${java.security.auth.login.config}")
    private String authLoginConfig;

    @Bean
    public KafkaSaslConfig generateKafkaSaslConfig() {
        if (System.getProperty("java.security.auth.login.config") == null) {
            System.setProperty("java.security.auth.login.config",authLoginConfig);
        }
        return new KafkaSaslConfig();
    }
}
