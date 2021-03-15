package com.admin.demo.configurations;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.admin.demo.KafkaAdminApplication;

@Configuration
public class ConfigurationService {

	@Value("${adminclient.url}")
	private String bootstrapServer;

	public AdminClient getAdminClient() {

		Properties adminconfig = new Properties();
		adminconfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		return AdminClient.create(adminconfig);
	}
	
	@Bean()
	public Logger logger() {
		return LoggerFactory.getLogger(KafkaAdminApplication.class);
	}
}
