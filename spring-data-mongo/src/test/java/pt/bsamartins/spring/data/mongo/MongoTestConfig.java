package pt.bsamartins.spring.data.mongo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.GenericContainer;

@Configuration
public class MongoTestConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MongoTestConfig.class);
    private static final int CONTAINER_PORT = 27017;

    @Bean
    public GenericContainer mongoContainer () {
        GenericContainer container = new GenericContainer("mongo:latest")
                .withExposedPorts(CONTAINER_PORT);
        container.start();
        LOG.info("Started Mongo on port {}", mongoPort(container));
        return container;
    }

    @Bean
    public int mongoPort(GenericContainer mongoContainer) {
        return mongoContainer.getMappedPort(CONTAINER_PORT);
    }


}
