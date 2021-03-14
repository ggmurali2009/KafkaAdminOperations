package com.admin.demo.entities;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaTopicInfo {

	private String topicName;
	
	@Autowired
	private List<PartitionInfo> partition;

	public KafkaTopicInfo() {
		super();
	}

	public KafkaTopicInfo(String topicName, List<PartitionInfo> partition) {
		super();
		this.topicName = topicName;
		this.partition = partition;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public List<PartitionInfo> getPartition() {
		return partition;
	}

	public void setPartition(List<PartitionInfo> partition) {
		this.partition = partition;
	}

	@Override
	public String toString() {
		return "KafkaTopicInfo [topicName=" + topicName + ", partition=" + partition + "]";
	}
	
	
	
	


}
