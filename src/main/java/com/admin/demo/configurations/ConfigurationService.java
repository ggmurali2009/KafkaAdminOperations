package com.admin.demo.configurations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.admin.demo.KafkaAdminApplication;
import com.admin.demo.entities.TopicResponse;
import com.admin.demo.entities.Topics;

@Configuration
public class ConfigurationService {

	@Value("${adminclient.url}")
	private String bootstrapServer;
	
	
	@Bean(value="properties")
	@Scope("prototype")
	public Properties getProperties() {
		return new Properties();
	}

	@Bean()
	@Scope("prototype")
	public List<Topics> topicList() {
		return new ArrayList<Topics>();
	}

	@Autowired
	@Qualifier("properties")
	private Properties adminconfig;

	@Bean()
	public AdminClient adminClient() {

		adminconfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		adminconfig.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		return AdminClient.create(adminconfig);
	}

	@Bean()
	public Logger logger() {

		return LoggerFactory.getLogger(KafkaAdminApplication.class);
	}

	@Bean()
	public Map<String, String> topicConfig() {

		return new HashMap<String, String>();
	}
	
	@Bean("prototype")
	public TopicResponse topicResponse() {
		return new TopicResponse();
	}

}
