package com.admin.demo.controllers;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
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

	@Autowired
	TopicResponse topicResponse;

	@ExceptionHandler(InvalidReplicationFactorException.class)
	@ResponseStatus(HttpStatus.CONFLICT)
	public ResponseEntity<String> replicationFactorMoreThanAvailablBroker(InvalidReplicationFactorException exception) {
		return ResponseEntity.status(HttpStatus.CONFLICT).body(exception.getMessage());
	}

	@ExceptionHandler(TopicExistsException.class)
	@ResponseStatus(HttpStatus.CONFLICT)
	public ResponseEntity<TopicResponse> topicAlreadyAvailableInBroker(TopicExistsException exception) {

		topicResponse.setStatus(Status.FAILURE);
		topicResponse.setMessage(ResponseMessage.TOPIC_AlREADY_EXISTS);
		topicResponse.setDetailedMessage(exception.getMessage());
		return ResponseEntity.status(HttpStatus.CONFLICT).body(topicResponse);
	}

	@ExceptionHandler(UnknownTopicOrPartitionException.class)
	@ResponseStatus(HttpStatus.NOT_FOUND)
	public ResponseEntity<TopicResponse> topicNotExists(UnknownTopicOrPartitionException exception) {
		topicResponse.setStatus(Status.FAILURE);
		topicResponse.setMessage(ResponseMessage.TOPIC_DOES_NOT_EXISTS);
		topicResponse.setDetailedMessage(exception.getMessage());
		return ResponseEntity.status(HttpStatus.NOT_FOUND).body(topicResponse);
	}
}
