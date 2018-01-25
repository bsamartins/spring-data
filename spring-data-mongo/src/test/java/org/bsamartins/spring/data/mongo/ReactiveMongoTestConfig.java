package org.bsamartins.spring.data.mongo;

import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;

@Configuration
public class ReactiveMongoTestConfig extends AbstractReactiveMongoConfiguration {

    @Autowired
    private int mongoPort;

    @Bean
    @Override
    public MongoClient reactiveMongoClient() {
        String connectionString = String.format("mongodb://localhost:%d", mongoPort);
        return MongoClients.create(connectionString);
    }

    @Bean
    @Override
    protected String getDatabaseName() {
        return "test-reactive";
    }
}
