package com.admin.demo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.admin.demo.entities.CreateTopicInfo;
import com.admin.demo.entities.KafkaTopicInfo;
import com.admin.demo.entities.PartitionInfo;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.enums.Status;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ExtendWith(SpringExtension.class)
class KafkaAdminApplicationTests {

	@Autowired
	TestRestTemplate testRestTemplate;

	@LocalServerPort
	private int port;

	public static final ObjectMapper mapper = new ObjectMapper();

	@BeforeEach
	public void cleanupCluster() throws InterruptedException, ExecutionException {
		AdminClient adminClient = getAdminClient();

		adminClient.deleteTopics(adminClient.listTopics().names().get()).all().get();

		adminClient.close();

	}

	@DisplayName("Test shoud pass when topic gets created")
	@Test
	public void testCreateTopic() throws IOException, InterruptedException, ExecutionException {
		AdminClient adminClient = getAdminClient();
		CreateTopicInfo event = mapper.readValue(
				KafkaAdminApplicationTests.class.getResourceAsStream("/sampleCreateTopicJson.json"),
				CreateTopicInfo.class);

		String payLoad = mapper.writeValueAsString(event);

		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<String> entity = new HttpEntity<String>(payLoad, headers);

		ResponseEntity<String> response = testRestTemplate.exchange("http://localhost:" + port + "/v1/topic",
				HttpMethod.POST, entity, String.class);
		assertTrue("Failed beacuse Response Code is not 200", response.getStatusCode().is2xxSuccessful());
		assertTrue("Topic not exists in Kafka cluster",
				adminClient.listTopics().names().get().contains(event.getTopicName()));

		adminClient.close();

	}

	@DisplayName("Test Should Pass on Invalid Topic configuration")
	@Test
	public void testInvalidCreateTopic() throws IOException, InterruptedException, ExecutionException {

		CreateTopicInfo event = mapper.readValue(
				KafkaAdminApplicationTests.class.getResourceAsStream("/CreateTopicwithoutPartitionInfoJson.json"),
				CreateTopicInfo.class);

		String payLoad = mapper.writeValueAsString(event);

		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<String> entity = new HttpEntity<String>(payLoad, headers);

		ResponseEntity<String> response = testRestTemplate.exchange("http://localhost:" + port + "/v1/topic",
				HttpMethod.POST, entity, String.class);
		String publishResponse = response.getBody();

		assertEquals(
				"{\"topic\":null,\"status\":\"FAILURE\",\"message\":\"Topic Configuration Error\",\"detailedMessage\":\"Number of partitions must be larger than 0.\"}",
				publishResponse);
		assertTrue("Topic Got created in Kafka cluster", response.getStatusCode().is4xxClientError());
		assertTrue("Topic Not created because of configuration ERR", response.getStatusCode().is4xxClientError());

		AdminClient adminClient = getAdminClient();

		assertFalse("Topic created on Kafka cluster",
				adminClient.listTopics().names().get().contains(event.getTopicName()));

		adminClient.close();

	}

	@DisplayName("Test shoud pass when existing topic gets deleted")
	@Test
	public void testDeleteTopic() throws IOException, InterruptedException, ExecutionException {
		CreateTopicInfo event = mapper.readValue(
				KafkaAdminApplicationTests.class.getResourceAsStream("/TestDeleteTopicJson.json"),
				CreateTopicInfo.class);

		String payLoad = mapper.writeValueAsString(event);

		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<String> entity = new HttpEntity<String>(payLoad, headers);

		createTopic("TestDeleteTopic");

		ResponseEntity<TopicResponse> response = testRestTemplate.exchange("http://localhost:" + port + "/v1/topic",
				HttpMethod.DELETE, entity, TopicResponse.class);

		TopicResponse topicResponse = response.getBody();
		assertTrue("Failed while deleting-beacuse Response Code is not 200",
				response.getStatusCode().is2xxSuccessful());
		assertEquals(Status.SUCCESS, topicResponse.getStatus());

		AdminClient adminClient = getAdminClient();
		assertFalse("Topic is not deleted from Kafka cluster",
				adminClient.listTopics().names().get().contains(event.getTopicName()));

		adminClient.close();

	}

	@DisplayName("Test should pass describing existing Topics")
	@Test
	public void testDescibeTopic() throws IOException, InterruptedException, ExecutionException {
		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", MediaType.APPLICATION_JSON_VALUE);
		HttpEntity<String> entity = new HttpEntity<String>(headers);

		/*-
		 * creating test topic 
		 * with name TestDescribeTopic
		 * partitions 3
		 * replication Factor 3
		 * */
		String topicName = "TestDescribeTopic";
		createTopic(topicName);

		ResponseEntity<KafkaTopicInfo> response = testRestTemplate.exchange(
				"http://localhost:" + port + "/v1/describe/TestDescribeTopic", HttpMethod.GET, entity,
				KafkaTopicInfo.class);
		/*
		 * assertTrue("Failed while deleting-beacuse Response Code is not 200",
		 * response.getStatusCode().is2xxSuccessful());
		 */
		AdminClient adminClient = getAdminClient();
		assertEquals(topicName, response.getBody().getTopicName());
		assertEquals(3, response.getBody().getPartition().size());
		assertEquals(3, response.getBody().getPartition().get(0).getReplicas().size());

		adminClient.close();

	}

	public AdminClient getAdminClient() {

		Properties adminconfig = new Properties();
		adminconfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		return AdminClient.create(adminconfig);
	}

	public void createTopic(String topicName) {
		AdminClient adminClient = getAdminClient();
		NewTopic newtopic = new NewTopic(topicName, 3, (short) 3);
		adminClient.createTopics(Arrays.asList(newtopic));
		adminClient.close();
	}

}
