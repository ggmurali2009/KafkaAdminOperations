package com.admin.demo.services;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.admin.demo.entities.TopicResponse;
import com.admin.demo.enums.ResponseMessage;
import com.admin.demo.enums.Status;

@Service
public class KafkaService {

	@Autowired
	Logger logger;
	
	@Autowired
	TopicResponse topicResponse;

	/*-
	  Deletes a topic 
	  Accepts : TopicName as String , AdminClient Object 
	  Returns : Void
	 */
	public void deleteTopics(List<String> topicsForDeletion, AdminClient adminclient)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <deleteTopics> in kafkaService");

		adminclient.deleteTopics(topicsForDeletion).all().get();

	}

	/*-
	  Creates a topic 
	  Accepts : Topic list with topic Informations , AdminClient Object 
	  Returns : CreateTopicsResult
	 */
	public void createTopics(Collection<NewTopic> topicsForcreation, AdminClient adminclient) throws InterruptedException, ExecutionException {
		logger.info("Inside <createTopics> Method in <KafkaService>");
			adminclient.createTopics(topicsForcreation).all().get();
		
	}

	/*-
	  Lists all the topic 
	  Accepts : AdminClient Object 
	  Returns : List of topic name as String
	 */
	public Set<String> getListTopics(AdminClient adminclient) throws InterruptedException, ExecutionException {

		return adminclient.listTopics().names().get();

	}

	/*-
	  Describe all the topic 
	  Accepts : AdminClient Object , Topic Names
	  Returns : Topic Description
	 */

	public Map<String, TopicDescription> describeTopics(AdminClient adminclient, Collection<String> topicName)
			throws InterruptedException, ExecutionException {
		return adminclient.describeTopics(topicName).all().get();
	}

	public boolean isTopicExists(AdminClient adminClient, String topicName)
			throws InterruptedException, ExecutionException {

		logger.info("Inside <isTopicExists> in <KafkaService>");
		
		if (getListTopics(adminClient).contains(topicName)) {
			return true;
		} else {
			return false;
		}

	}
	
	public TopicResponse buildResponse(Status status,String responseMessage) {
		
		topicResponse.setStatus(status);
		topicResponse.setMessage(responseMessage);
		
		return topicResponse;
	}

}
