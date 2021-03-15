package com.admin.demo.entities;

import java.util.ArrayList;
import java.util.List;

public class KafkaTopicInfo {

	private String topicName;

	private List<PartitionInfo> partition = new ArrayList<>();

	public KafkaTopicInfo() {
	}

	public KafkaTopicInfo(String topicName, List<PartitionInfo> partition) {
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
