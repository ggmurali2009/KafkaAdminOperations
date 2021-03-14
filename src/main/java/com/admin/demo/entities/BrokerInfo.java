package com.admin.demo.entities;

import org.springframework.stereotype.Component;

@Component
public class BrokerInfo {
	
	private int brokerId;

	public BrokerInfo() {
		super();
	}

	public BrokerInfo(int brokerId) {
		super();
		this.brokerId = brokerId;
	}

	public int getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(int brokerId) {
		this.brokerId = brokerId;
	}

	@Override
	public String toString() {
		return "BrokerInfo [brokerId=" + brokerId + "]";
	}
	
	

}
