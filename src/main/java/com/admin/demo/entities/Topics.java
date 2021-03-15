package com.admin.demo.entities;

public class Topics {

	private String topicName;

	public Topics() {
		super();
	}

	public Topics(String topicName) {
		super();
		this.topicName = topicName;
	}

	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	@Override
	public String toString() {
		return "Topics [topicName=" + topicName + "]";
	}

}
