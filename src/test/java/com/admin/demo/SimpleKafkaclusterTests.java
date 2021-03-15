package com.admin.demo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaBroker.EmbeddedZookeeper;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, controlledShutdown = false, brokerProperties = { "listeners=PLAINTEXT://localhost:9092",
		"port=9092" })
@ExtendWith(SpringExtension.class)
public class SimpleKafkaclusterTests {
	
	private static final Logger log = LoggerFactory.getLogger(SimpleKafkaclusterTests.class);
    private EmbeddedZookeeper zookeeper = null;
    private EmbeddedKafka broker = null;
    
    @Test
    public void test1(){
    	
    	EmbeddedKafkaBroker n= new EmbeddedKafkaBroker(1);

    	
    	
    }

}
