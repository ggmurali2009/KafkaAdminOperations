package com.admin.demo.entities;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class AdminClientConfiguration {

	private AdminClient adminClient;

	@Autowired
	@Value("${adminclient.url}")
	private String bootstrapServer;

	@Autowired
	@Qualifier("properties")
	Properties properties;

	public AdminClientConfiguration() {
		super();
	}

	public String getBootstrapServer() {
		return bootstrapServer;
	}

	public void setBootstrapServer(String bootstrapServer) {
		this.bootstrapServer = bootstrapServer;
	}

	public AdminClient getAdminClient() {
		properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServer());
		adminClient = AdminClient.create(properties);
		return adminClient;

	}

}
