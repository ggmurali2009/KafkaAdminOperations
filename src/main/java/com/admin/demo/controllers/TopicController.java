package com.admin.demo.controllers;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.admin.demo.entities.CreateTopicInfo;
import com.admin.demo.entities.KafkaTopicInfo;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.entities.Topics;
import com.admin.demo.services.KafkaService;

@RestController
@RequestMapping("/v1")
public class TopicController {

	@Autowired
	KafkaService kafkaService;

	@Autowired
	Logger logger;

	/* creates New topic into KAFKA cluster */
	@PostMapping(value = "/topic", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public TopicResponse createTopic(@RequestBody CreateTopicInfo createTopicInfo)
			throws InterruptedException, ExecutionException {
		logger.info("Inside <createTopic> method in <controller> - START");
		/* Calling createTopics method to create New topic */
		return kafkaService.createTopic(createTopicInfo);

	}

	/* listing all the topics in the KAFKA cluster */
	@GetMapping(value = "/topics", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Topics> listAllTopics() throws InterruptedException, ExecutionException {

		logger.info("Inside <listAllTopics> method in <controller> - END");
		/* Calling getListTopics method to list all the topics in the KAFKA cluster */
		return kafkaService.listTopics();
	}

	/* Describe a particular Topic */
	@GetMapping(value = "/describe/{topicname}")
	public KafkaTopicInfo describe(@PathVariable String topicname) throws InterruptedException, ExecutionException {
		logger.info("Inside <describeTopics> method of <Controller> - Start");
		return kafkaService.describeTopics(Arrays.asList(topicname));

	}

	/* deletes the topic provided */
	@DeleteMapping(value = "/topic", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public TopicResponse deleteTopic(@RequestBody Topics topicsForDeletion)
			throws InterruptedException, ExecutionException {

		/* Calling kafkaService to delete particular Topic */
		return kafkaService.deleteTopics(topicsForDeletion);

	}

	/* Describes all the Topics in the KAFKA cluster */
	@GetMapping(value = "/describeall")
	public List<KafkaTopicInfo> describeTopics() throws InterruptedException, ExecutionException {
		logger.info("Inside <describeTopics> method of <Controller> - Start");
		return kafkaService.describeAllTopics(kafkaService.listTopics());
	}

	/* Bulk creation of New topics into KAFKA cluster */
	@PostMapping(value = "/topics", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public List<TopicResponse> createMoreTopic(@RequestBody List<CreateTopicInfo> listOfTopicsForCreation)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <createMoreTopic> method in <controller>");
		/* Creating New topics in KAFKA */
		return kafkaService.createTopics(listOfTopicsForCreation);

	}

	/* Bulk topic deletion */
	@DeleteMapping(value = "/topics", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public List<TopicResponse> deleteMoreTopic(@RequestBody List<Topics> topicsForDeletion)
			throws InterruptedException, ExecutionException {
		return kafkaService.deleteMoreTopics(topicsForDeletion);
	}

}
