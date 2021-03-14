package com.admin.demo.entities;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CreateTopicInfo {

	private String bootstrapServer;
	private String topicName;
	private int partitions;
	private short replicationFactor;
	@Autowired
	private Map<String, String> topicConfig;

	public CreateTopicInfo() {
		super();
	}

	public CreateTopicInfo(String topicName, int partitions, short replicationFactor, Map<String, String> topicConfig) {
		super();
		this.topicName = topicName;
		this.partitions = partitions;
		this.replicationFactor = replicationFactor;
		this.topicConfig = topicConfig;
	}

	
	
	public String getBootstrapServer() {
		return bootstrapServer;
	}

	public void setBootstrapServer(String bootstrapServer) {
		this.bootstrapServer = bootstrapServer;
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
