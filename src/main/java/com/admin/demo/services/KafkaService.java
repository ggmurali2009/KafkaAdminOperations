package com.admin.demo.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.admin.demo.configurations.ConfigurationService;
import com.admin.demo.entities.BrokerInfo;
import com.admin.demo.entities.CreateTopicInfo;
import com.admin.demo.entities.KafkaTopicInfo;
import com.admin.demo.entities.PartitionInfo;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.entities.Topics;
import com.admin.demo.enums.ResponseMessage;
import com.admin.demo.enums.Status;

@Service
public class KafkaService {

	@Autowired
	Logger logger;

	@Autowired
	ConfigurationService configurationService;

	/*
	 * @Autowired AdminClientConfiguration adminClientConfiguration;
	 */

	/*-
	  Deletes a topic 
	  Accepts : TopicName as String , AdminClient Object 
	  Returns : Void
	 */
	public TopicResponse deleteTopics(Topics topicsForDeletion) throws InterruptedException, ExecutionException {

		logger.info("Inside <deleteTopics> in kafkaService");

		AdminClient adminClient = configurationService.getAdminClient();

		adminClient.deleteTopics(Arrays.asList(topicsForDeletion.getTopicName())).all().get();

		adminClient.close();
		return buildResponse(topicsForDeletion.getTopicName(), Status.SUCCESS, ResponseMessage.TOPIC_DELETED,
				ResponseMessage.TOPIC_DELETED);
	}

	/*-
	  Bulk Deleted the topics 
	  Accepts : TopicName as String , AdminClient Object 
	  Returns : Void
	 */
	public List<TopicResponse> deleteMoreTopics(List<Topics> topicsForDeletion)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <deleteTopics> in kafkaService");

		List<TopicResponse> topicResponse = new ArrayList<>();

		for (Topics topics : topicsForDeletion) {
			topicResponse.add(deleteTopics(topics));

		}
		return topicResponse;
	}

	/*- Creates a topic Accepts : Topic list with topic Informations ,
	 * AdminClient Object Returns : CreateTopicsResult
	 */
	public TopicResponse createTopic(CreateTopicInfo createTopicInfo) throws InterruptedException, ExecutionException {
		logger.info("Inside <createTopics> Method in <KafkaService>");

		AdminClient adminClient = configurationService.getAdminClient();

		adminClient
				.createTopics(
						Arrays.asList(new NewTopic(createTopicInfo.getTopicName(), createTopicInfo.getPartitions(),
								createTopicInfo.getReplicationFactor()).configs(createTopicInfo.getTopicConfig())))
				.all().get();

		adminClient.close();
		return buildResponse(createTopicInfo.getTopicName(), Status.SUCCESS, ResponseMessage.TOPIC_CREATED,
				ResponseMessage.TOPIC_CREATED);

	}

	/*- Creates a topic Accepts : Topic list with topic Informations ,
	 * AdminClient Object Returns : CreateTopicsResult
	 */
	public List<TopicResponse> createTopics(List<CreateTopicInfo> listOfTopicsForCreation)
			throws InterruptedException, ExecutionException {
		logger.info("Inside <createTopics> Method in <KafkaService>");

		List<TopicResponse> topicResponseList = new ArrayList<>();

		/* Creating New topics in KAFKA */
		for (CreateTopicInfo createTopicInfo : listOfTopicsForCreation) {
			topicResponseList.add(createTopic(createTopicInfo));
		}
		return topicResponseList;
	}

	/*-
	  Lists all the topic 
	  Accepts : AdminClient Object 
	  Returns : List of topic name as String
	 */
	public List<Topics> listTopics() throws InterruptedException, ExecutionException {

		AdminClient adminClient = configurationService.getAdminClient();
		List<Topics> topicLists = new ArrayList<>();
		adminClient.listTopics().names().get().forEach(topic -> {
			topicLists.add(new Topics(topic));
		});
		adminClient.close();
		return topicLists;

	}

	/*-
	  Describe a topic 
	  Accepts : AdminClient Object , Topic Names
	  Returns : Topic Description
	 */

	public KafkaTopicInfo describeTopics(Collection<String> topicName) throws InterruptedException, ExecutionException {
		AdminClient adminClient = configurationService.getAdminClient();

		Map<String, TopicDescription> m = adminClient.describeTopics(topicName).all().get();

		KafkaTopicInfo kafkaTopicInfo = null;
		PartitionInfo partitionInfo = null;
		BrokerInfo brokerInfo = null;

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
			}
			kafkaTopicInfo.setPartition(paritionInfoList);
		}
		adminClient.close();
		return kafkaTopicInfo;
	}

	/*-
	  Describe all the topic 
	  Accepts : AdminClient Object , Topic Names
	  Returns : Topic Description
	 */

	public List<KafkaTopicInfo> describeAllTopics(List<Topics> topicName)
			throws InterruptedException, ExecutionException {
		AdminClient adminClient = configurationService.getAdminClient();
		Map<String, TopicDescription> m = adminClient.describeTopics(adminClient.listTopics().names().get()).all()
				.get();

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
			}
			kafkaTopicInfo.setPartition(paritionInfoList);
			kafkaTopicInfoList.add(kafkaTopicInfo);
		}
		adminClient.close();
		return kafkaTopicInfoList;
	}

	/*-
	  constructing TopicResponce
	  Accepts : Topic Names,Status,Message
	  Returns : TopicResponse
	 */

	public TopicResponse buildResponse(String topicName, Status status, String responseMessage,
			String detailedMessage) {
		TopicResponse topicResponse = new TopicResponse();
		topicResponse.setTopic(topicName);
		topicResponse.setStatus(status);
		topicResponse.setMessage(responseMessage);
		topicResponse.setDetailedMessage(detailedMessage);

		return topicResponse;
	}

}
