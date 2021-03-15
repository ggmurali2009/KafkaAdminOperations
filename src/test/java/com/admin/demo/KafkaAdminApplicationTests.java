package com.admin.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, controlledShutdown = false, brokerProperties = { "listeners=PLAINTEXT://localhost:9092",
		"port=9092" })
@ExtendWith(SpringExtension.class)
class KafkaAdminApplicationTests {

	private EmbeddedKafkaBroker embeddedKafkaBroker=new EmbeddedKafkaBroker(1);

	public static final Logger LOGGER = LoggerFactory.getLogger(KafkaAdminApplicationTests.class);

	// TestRestTemplate testRestTemplate = new TestRestTemplate();

	// public static final ObjectMapper mapper = new ObjectMapper();

	private static final String TEST_TOPIC = "testTopic";

	@BeforeEach
	void setUp(EmbeddedKafkaBroker embeddedKafkaBroker) {
		this.embeddedKafkaBroker = embeddedKafkaBroker;
	}

	@Test
	void createTopicTest() {
		LOGGER.info("Inside <createTopicTest>");
		/*
		 * 
		 * 
		 * Map<String, Object> configs = new
		 * HashMap<>(KafkaTestUtils.consumerProps("consumer", "false",
		 * embeddedKafkaBroker)); DefaultKafkaConsumerFactory<String, String>
		 * consumerFactory = new DefaultKafkaConsumerFactory<>( configs, new
		 * StringDeserializer(), new StringDeserializer() );
		 * 
		 * 
		 * HotelResourceInfo event = mapper.readValue(
		 * ReportGeneratorTest.class.getResourceAsStream("/SampleResource.json"),
		 * HotelResourceInfo.class); String jwt = addHotelUserAndGenerateJWT(); String
		 * payLoad = mapper.writeValueAsString(event);
		 * LOGGER.debug("Publishing  resource {}  ", payLoad); HttpHeaders headers = new
		 * HttpHeaders(); headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
		 * headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + jwt); HttpEntity<String>
		 * entity = new HttpEntity<String>(payLoad, headers); ResponseEntity<String>
		 * response = testRestTemplate.exchange( "http://localhost:" + port +
		 * "/v1.0/hotel/resource/hotel123", HttpMethod.POST, entity, String.class);
		 * String publishResponse = response.getBody();
		 * LOGGER.debug("Response for publishing  resource {}  ", publishResponse);
		 * Assert.assertTrue("Resource report not published",
		 * response.getStatusCode().is2xxSuccessful());
		 * 
		 * String eventFromKafka = kafkaConsumerClient.readKafkaValue("reports");
		 * LOGGER.debug("Event from Kafka {}  ", eventFromKafka); HotelResourceInfo
		 * eventPublished = mapper.readValue(eventFromKafka, HotelResourceInfo.class);
		 * Assert.assertTrue("Hotel id not matched ",
		 * eventPublished.getHotelId().equals(event.getHotelId()));
		 * Assert.assertTrue("Hotel id not matched ",
		 * eventPublished.getHotelName().equals(event.getHotelName()));
		 * Assert.assertTrue("Incorrect resources received ",
		 * eventPublished.getResources().size() == 3);
		 */ }

	@Test
	public void testReceivingKafkaEvents() {
		Consumer<Integer, String> consumer = configureConsumer();
		Producer<Integer, String> producer = configureProducer();
		embeddedKafkaBroker=new EmbeddedKafkaBroker(1);

		producer.send(new ProducerRecord<>(TEST_TOPIC, 123, "my-test-value"));

		ConsumerRecord<Integer, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, TEST_TOPIC);
		assertThat(singleRecord).isNotNull();
		assertThat(singleRecord.key()).isEqualTo(123);
		assertThat(singleRecord.value()).isEqualTo("my-test-value");

		consumer.close();
		producer.close();
	}

	private Consumer<Integer, String> configureConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafkaBroker);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		Consumer<Integer, String> consumer = new DefaultKafkaConsumerFactory<Integer, String>(consumerProps)
				.createConsumer();
		consumer.subscribe(Collections.singleton(TEST_TOPIC));
		return consumer;
	}

	private Producer<Integer, String> configureProducer() {
		Map<String, Object> producerProps = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
		return new DefaultKafkaProducerFactory<Integer, String>(producerProps).createProducer();
	}

}
