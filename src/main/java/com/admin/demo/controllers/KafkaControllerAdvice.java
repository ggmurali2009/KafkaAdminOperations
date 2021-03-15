package com.admin.demo.controllers;

import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.admin.demo.entities.TopicResponse;
import com.admin.demo.enums.ResponseMessage;
import com.admin.demo.enums.Status;

@ControllerAdvice
public class KafkaControllerAdvice {

	@ExceptionHandler(InvalidReplicationFactorException.class)
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public ResponseEntity<TopicResponse> replicationFactorMoreThanAvailablBroker(
			InvalidReplicationFactorException exception) {

		TopicResponse topicResponse = buildResponse(null, Status.FAILURE, ResponseMessage.TOPIC_CONFIG_ERROR,
				exception.getMessage());
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(topicResponse);
	}

	@ExceptionHandler(TopicExistsException.class)
	@ResponseStatus(HttpStatus.CONFLICT)
	public ResponseEntity<TopicResponse> topicAlreadyAvailableInBroker(TopicExistsException exception) {

		TopicResponse topicResponse = buildResponse(null, Status.FAILURE, ResponseMessage.TOPIC_AlREADY_EXISTS,
				exception.getMessage());
		return ResponseEntity.status(HttpStatus.CONFLICT).body(topicResponse);
	}

	@ExceptionHandler(UnknownTopicOrPartitionException.class)
	@ResponseStatus(HttpStatus.NOT_FOUND)
	public ResponseEntity<TopicResponse> topicNotExists(UnknownTopicOrPartitionException exception) {

		TopicResponse topicResponse = buildResponse(null, Status.FAILURE, ResponseMessage.TOPIC_DOES_NOT_EXISTS,
				exception.getMessage());
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(topicResponse);
	}

	@ExceptionHandler(InvalidPartitionsException.class)
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public ResponseEntity<TopicResponse> topicParitionInvalid(InvalidPartitionsException exception) {

		TopicResponse topicResponse = buildResponse(null, Status.FAILURE, ResponseMessage.TOPIC_CONFIG_ERROR,
				exception.getMessage());
		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(topicResponse);
	}

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
