package com.admin.demo.entities;

import java.util.HashMap;
import java.util.Map;

public class CreateTopicInfo {

	private String topicName;
	private int partitions;
	private short replicationFactor;
	private Map<String, String> topicConfig = new HashMap<>();

	public CreateTopicInfo() {
	}

	public CreateTopicInfo(String topicName, int partitions, short replicationFactor, Map<String, String> topicConfig) {
		this.topicName = topicName;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		this.topicConfig = topicConfig;
	}
	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public int getPartitions() {
		return partitions;
	}

	public void setPartitions(int partitions) {
		this.partitions = partitions;
	}

	public short getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(short replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public Map<String, String> getTopicConfig() {
		return topicConfig;
	}

	public void setTopicConfig(Map<String, String> topicConfig) {
		this.topicConfig = topicConfig;
	}

	@Override
	public String toString() {
		return "CreateTopicInfo [topicName=" + topicName + ", partitions=" + partitions + ", replicationFactor="
				+ replicationFactor + ", topicConfig=" + topicConfig + "]";
	}

}
