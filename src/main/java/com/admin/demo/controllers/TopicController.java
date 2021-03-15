package com.admin.demo.controllers;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.admin.demo.entities.AdminClientConfiguration;
import com.admin.demo.entities.CreateTopicInfo;
import com.admin.demo.entities.KafkaTopicInfo;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.entities.Topics;
import com.admin.demo.services.KafkaService;

/*-
 * 
 * Rest end points available:
 * 			/topic/create 
 *          /topic/createmore 
 *          /topic/listtopics
 *          /topic/describe
 *          /topic/describeall
 *          /topic/delete
 * 
 */

@RestController
@RequestMapping("/topic")
public class TopicController {

	@Autowired
	AdminClientConfiguration adminClientConfiguration;

	@Autowired
	KafkaService kafkaService;

	@Autowired
	AdminClient adminClient;

	@Autowired
	Logger logger;

	/* creates New topic into KAFKA cluster */
	@PostMapping(value = "/create", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public TopicResponse createTopic(@RequestBody CreateTopicInfo createTopicInfo)
			throws InterruptedException, ExecutionException {
		logger.info("Inside <createTopic> method in <controller> - START");
		/* Calling createTopics method to create New topic */
		return kafkaService.createTopics(createTopicInfo, adminClient);

	}

	/* listing all the topics in the KAFKA cluster */
	@GetMapping(value = "/listtopics", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Topics> listAllTopics() throws InterruptedException, ExecutionException {

		logger.info("Inside <listAllTopics> method in <controller> - END");
		/* Calling getListTopics method to list all the topics in the KAFKA cluster */
		return kafkaService.getListTopics(adminClient);
	}

	/* Describe a particular Topic */
	@GetMapping(value = "/describe", consumes = MediaType.APPLICATION_JSON_VALUE)
	public KafkaTopicInfo describe(@RequestBody Topics topic) throws InterruptedException, ExecutionException {
		logger.info("Inside <describeTopics> method of <Controller> - Start");
		return kafkaService.describeTopics(adminClient, Arrays.asList(topic.getTopicName()));

	}

	/* deletes the topic provided */
	@DeleteMapping(value = "/delete", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public TopicResponse deleteTopic(@RequestBody Topics topicsForDeletion)
			throws InterruptedException, ExecutionException {

		/* Calling kafkaService to delete particular Topic */
		return kafkaService.deleteTopics(topicsForDeletion, adminClient);

	}

	/* Describes all the Topics in the KAFKA cluster */
	@GetMapping(value = "/describeall", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<KafkaTopicInfo> describeTopics() throws InterruptedException, ExecutionException {
		logger.info("Inside <describeTopics> method of <Controller> - Start");
		return kafkaService.describeAllTopics(adminClient, kafkaService.getListTopics(adminClient));
	}

	/* Bulk creation of New topics into KAFKA cluster */
	@PostMapping(value = "/createmore", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public List<TopicResponse> createMoreTopic(@RequestBody List<CreateTopicInfo> listOfTopicsForCreation)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <createMoreTopic> method in <controller>");
		/* Creating New topics in KAFKA */
		return kafkaService.createMoreTopics(listOfTopicsForCreation, adminClient);

	}

	/* Bulk topic deletion */
	@DeleteMapping(value = "/deletemore", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public List<TopicResponse> deleteMoreTopic(@RequestBody List<Topics> topicsForDeletion)
			throws InterruptedException, ExecutionException {
		return kafkaService.deleteMoreTopics(topicsForDeletion, adminClient);
	}

}
