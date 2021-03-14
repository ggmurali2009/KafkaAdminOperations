package com.admin.demo.controllers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
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
import com.admin.demo.entities.BrokerInfo;
import com.admin.demo.entities.CreateTopicInfo;
import com.admin.demo.entities.KafkaTopicInfo;
import com.admin.demo.entities.PartitionInfo;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.entities.Topics;
import com.admin.demo.enums.ResponseMessage;
import com.admin.demo.enums.Status;
import com.admin.demo.services.KafkaService;

/*-
 * 
 * 
 * Rest end points available:
 * 			/create -
 *          /createmore -   
 * 
 * 
 * 
 * 
 */

@RestController
@RequestMapping("/topic")
public class TopicController {

	@Autowired
	KafkaTopicInfo kafkaTopicInfo;

	@Autowired
	CreateTopicInfo createTopicInfo;

	@Autowired
	AdminClientConfiguration adminClientConfiguration;

	@Autowired
	KafkaService kafkaService;

	@Autowired
	AdminClient adminClient;

	Set<String> topicSet;

	@Autowired
	List<Topics> topicLists;

	@Autowired
	Logger logger;

	/* creates New topic into KAFKA cluster */
	@PostMapping(value = "/create", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public TopicResponse createTopic(@RequestBody CreateTopicInfo createTopicInfo)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <createTopic> method in <controller> - START");

		adminClientConfiguration.setBootstrapServer(createTopicInfo.getBootstrapServer());
		adminClient = adminClientConfiguration.getAdminClient();

		/* Calling createTopics method to create New topic */
		kafkaService.createTopics(
				Arrays.asList(new NewTopic(createTopicInfo.getTopicName(), createTopicInfo.getPartitions(),
						createTopicInfo.getReplicationFactor()).configs(createTopicInfo.getTopicConfig())),
				adminClient);

		logger.info("Inside <createTopic> method in <controller> - END");
		return kafkaService.buildResponse(Status.SUCCESS, ResponseMessage.TOPIC_CREATED);
	}

	@PostMapping(value = "/createmore", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	public List<String> createMoreTopic(@RequestBody List<CreateTopicInfo> listOfTopicsForCreation)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <createMoreTopic> method in <controller>");

		List<String> returnString = new ArrayList<String>();
		List<CreateTopicInfo> validTopics = new ArrayList<>();

		/* Checking the given topic name is already exists in the KAFKA cluster */
		for (CreateTopicInfo createTopicInfo : listOfTopicsForCreation) {

			adminClientConfiguration.setBootstrapServer(createTopicInfo.getBootstrapServer());
			adminClient = adminClientConfiguration.getAdminClient();

			if (kafkaService.getListTopics(adminClient).contains(createTopicInfo.getTopicName())) {
				returnString.add("TOPIC ALREADY EXISTS." + createTopicInfo.getTopicName().toString());

			} else {
				validTopics.add(createTopicInfo);
			}
		}

		/* Creating New topics in KAFKA */
		for (CreateTopicInfo createTopicInfo : validTopics) {
			adminClientConfiguration.setBootstrapServer(createTopicInfo.getBootstrapServer());
			adminClient = adminClientConfiguration.getAdminClient();
			kafkaService.createTopics(
					Arrays.asList(new NewTopic(createTopicInfo.getTopicName(), createTopicInfo.getPartitions(),
							createTopicInfo.getReplicationFactor()).configs(createTopicInfo.getTopicConfig())),
					adminClient);
		}

		/*
		 * verifying whether the requested topic has been created in the cluster or not?
		 */

		for (CreateTopicInfo createTopicInfo : validTopics) {
			adminClientConfiguration.setBootstrapServer(createTopicInfo.getBootstrapServer());
			adminClient = adminClientConfiguration.getAdminClient();
			if (kafkaService.getListTopics(adminClient).contains(createTopicInfo.getTopicName())) {
				returnString.add(("Topic Created Successfully." + createTopicInfo.getTopicName().toString()));

			} else {
				createTopicInfo.getTopicName().toString();
				System.out.println(kafkaService.getListTopics(adminClient));
				System.out.println(kafkaService.getListTopics(adminClient));
				returnString.add(
						"Topic Not Created.Check the configurations. " + createTopicInfo.getTopicName().toString());
			}
		}
		return returnString;
	}

	/* listing all the topics in the KAFKA cluster */
	@GetMapping(value = "/listtopics", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<Topics> listAllTopics() throws InterruptedException, ExecutionException {

		topicLists.clear();
		kafkaService.getListTopics(adminClient).forEach(topic -> {
			topicLists.add(new Topics(topic));
		});
		return topicLists;

	}

	@GetMapping(value = "/describe", consumes = MediaType.APPLICATION_JSON_VALUE)
	public KafkaTopicInfo describe(@RequestBody Topics topic) throws InterruptedException, ExecutionException {

		logger.info("Inside <describeTopics> method of <Controller> - Start");

		Map<String, TopicDescription> m = kafkaService.describeTopics(adminClient, Arrays.asList(topic.getTopicName()));

		KafkaTopicInfo kafkaTopicInfo = null;
		PartitionInfo partitionInfo = null;
		BrokerInfo brokerInfo = null;

		List<PartitionInfo> paritionInfoList = null;
		List<BrokerInfo> replicaBrokerInfo = null;
		List<BrokerInfo> isrBrokerInfo = null;

		for (Map.Entry<String, TopicDescription> entry : m.entrySet()) {

			/* Assigning Topic Information */
			kafkaTopicInfo = new KafkaTopicInfo();
			kafkaTopicInfo.setTopicName(entry.getKey());

			/* Getting Partition Information */
			paritionInfoList = new ArrayList<PartitionInfo>();
			for (TopicPartitionInfo i : entry.getValue().partitions()) {

				partitionInfo = new PartitionInfo();

				/* Assigning Partition Number */
				partitionInfo.setPartition(i.partition());

				/* Assigning Partition Leader Information */
				brokerInfo = new BrokerInfo();
				brokerInfo.setBrokerId(i.leader().id());
				partitionInfo.setPartitionLeader(brokerInfo);

				/* Replica Broker Information */

				replicaBrokerInfo = new ArrayList<>();
				for (Node n : i.replicas()) {

					brokerInfo = new BrokerInfo();
					brokerInfo.setBrokerId(n.id());
					replicaBrokerInfo.add(brokerInfo);

				}
				partitionInfo.setReplicas(replicaBrokerInfo);

				/* ISR Broker Information */
				isrBrokerInfo = new ArrayList<>();
				for (Node n : i.isr()) {

					brokerInfo = new BrokerInfo();
					brokerInfo.setBrokerId(n.id());
					isrBrokerInfo.add(brokerInfo);
				}
				partitionInfo.setIsr(isrBrokerInfo);

				paritionInfoList.add(partitionInfo);
				// isrBrokerInfo.clear();
				// replicaBrokerInfo.clear();

			}
			kafkaTopicInfo.setPartition(paritionInfoList);
		}
		logger.info("Inside <describeTopics> method of <Controller> - End");
		return kafkaTopicInfo;

	}

	@GetMapping(value = "/describeall", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<KafkaTopicInfo> describeTopics() throws InterruptedException, ExecutionException {

		logger.info("Inside <describeTopics> method of <Controller> - Start");

		Map<String, TopicDescription> m = kafkaService.describeTopics(adminClient,
				kafkaService.getListTopics(adminClient));

		KafkaTopicInfo kafkaTopicInfo;
		PartitionInfo partitionInfo;
		BrokerInfo brokerInfo;

		List<KafkaTopicInfo> kafkaTopicInfoList = new ArrayList<>();
		List<PartitionInfo> paritionInfoList = new ArrayList<>();
		List<BrokerInfo> replicaBrokerInfo = new ArrayList<>();
		List<BrokerInfo> isrBrokerInfo = new ArrayList<>();

		for (Map.Entry<String, TopicDescription> entry : m.entrySet()) {

			/* Assigning Topic Information */
			kafkaTopicInfo = new KafkaTopicInfo();
			kafkaTopicInfo.setTopicName(entry.getKey());

			/* Getting Partition Information */
			paritionInfoList = new ArrayList<PartitionInfo>();
			for (TopicPartitionInfo i : entry.getValue().partitions()) {

				partitionInfo = new PartitionInfo();

				/* Assigning Partition Number */
				partitionInfo.setPartition(i.partition());

				/* Assigning Partition Leader Information */
				brokerInfo = new BrokerInfo();
				brokerInfo.setBrokerId(i.leader().id());
				partitionInfo.setPartitionLeader(brokerInfo);

				/* Replica Broker Information */

				replicaBrokerInfo = new ArrayList<>();
				for (Node n : i.replicas()) {

					brokerInfo = new BrokerInfo();
					brokerInfo.setBrokerId(n.id());
					replicaBrokerInfo.add(brokerInfo);

				}
				partitionInfo.setReplicas(replicaBrokerInfo);

				/* ISR Broker Information */
				isrBrokerInfo = new ArrayList<>();
				for (Node n : i.isr()) {

					brokerInfo = new BrokerInfo();
					brokerInfo.setBrokerId(n.id());
					isrBrokerInfo.add(brokerInfo);
				}
				partitionInfo.setIsr(isrBrokerInfo);

				paritionInfoList.add(partitionInfo);
				// isrBrokerInfo.clear();
				// replicaBrokerInfo.clear();

			}
			kafkaTopicInfo.setPartition(paritionInfoList);
			kafkaTopicInfoList.add(kafkaTopicInfo);
			// paritionInfoList.clear();

		}
		logger.info("Inside <describeTopics> method of <Controller> - End");
		return kafkaTopicInfoList;

	}

	/* deletes the topic */
	@DeleteMapping(value = "/delete", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public String deleteTopic(@RequestBody Topics topicsForDeletion) throws InterruptedException, ExecutionException {

		/* Calling kafkaService to delete particular Topic */

		kafkaService.deleteTopics(Arrays.asList(topicsForDeletion.getTopicName()), adminClient);

		/*
		 * verifying whether the requested topic has been created in the cluster or not
		 */
		if (!kafkaService.getListTopics(adminClient).contains(topicsForDeletion.getTopicName())) {
			return "Topic Deleted Successfully." + topicsForDeletion.getTopicName();

		} else {
			return "Topic Not Deleted. " + createTopicInfo.getTopicName().toString();

		}

	}

	/* Bulk topic deletion */
	@DeleteMapping(value = "/deletemore", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	public List<String> deleteMoreTopic(@RequestBody List<Topics> topicsForDeletion)
			throws InterruptedException, ExecutionException {

		List<String> returnString = new ArrayList<String>();
		List<Topics> validTopics = new ArrayList<Topics>();

		/* Checking the given topic name already exists in the KAFKA cluster */
		/*
		 * for (Topics topicList : topicsForDeletion) { if
		 * (!kafkaService.getListTopics(adminClient).contains(topicList.getTopicName()))
		 * { returnString.add("TOPIC DOES NOT EXISTS." +
		 * topicList.getTopicName().toString());
		 * 
		 * } else { validTopics.add(topicList);
		 * 
		 * } }
		 */

		for (Topics topicList : topicsForDeletion) {
			try {
				kafkaService.deleteTopics(Arrays.asList(topicList.getTopicName()), adminClient);
			} catch (InterruptedException | ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		for (Topics topicList : validTopics) {
			if (!kafkaService.getListTopics(adminClient).contains(topicList.getTopicName())) {
				returnString.add("Topics deleted sucessfully." + topicList.getTopicName().toString());

			} else {
				returnString.add("Topics Not deleted." + topicList.getTopicName().toString());

			}
		}

		return returnString;
	}

}
