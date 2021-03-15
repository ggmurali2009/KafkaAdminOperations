package com.admin.demo;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.Before;
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
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.admin.demo.entities.CreateTopicInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@ExtendWith(SpringExtension.class)
class KafkaAdminApplicationTests {

	@Autowired
	TestRestTemplate testRestTemplate;

	@LocalServerPort
	private int port;

	public static final ObjectMapper mapper = new ObjectMapper();

	@Before
	public void cleanupCluster() {

	}

	@Test
	public void testCreateTopic() throws IOException, InterruptedException, ExecutionException {

		CreateTopicInfo event = mapper.readValue(
				KafkaAdminApplicationTests.class.getResourceAsStream("/sampleCreateTopicJson.json"),
				CreateTopicInfo.class);

		String payLoad = mapper.writeValueAsString(event);

		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", "application/json");
		HttpEntity<String> entity = new HttpEntity<String>(payLoad, headers);

		ResponseEntity<String> response = testRestTemplate.exchange("http://localhost:" + port + "/v1/topic",
				HttpMethod.POST, entity, String.class);
		String publishResponse = response.getBody();

		// assertTrue("Topic Not created", response.getStatusCode().is2xxSuccessful());

		assertTrue("Topic Not created", response.getStatusCode().is4xxClientError());

		AdminClient adminClient = getAdminClient();

		assertTrue("Topic not exists in Kafka cluster",
				adminClient.listTopics().names().get().contains(event.getTopicName()));

		adminClient.close();

	}

	public AdminClient getAdminClient() {

		Properties adminconfig = new Properties();
		adminconfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		return AdminClient.create(adminconfig);
	}

}
