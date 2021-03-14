package com.admin.demo.entities;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PartitionInfo {
	private int partition;
	
	@Autowired
	private BrokerInfo partitionLeader;
	
	private List<BrokerInfo> replicas;
	private List<BrokerInfo> Isr;
	
	public PartitionInfo() {
		super();
	}

	public PartitionInfo(int partition, BrokerInfo partitionLeader, List<BrokerInfo> replicas, List<BrokerInfo> isr) {
		super();
		this.partition = partition;
		this.partitionLeader = partitionLeader;
		this.replicas = replicas;
		Isr = isr;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public BrokerInfo getPartitionLeader() {
		return partitionLeader;
	}

	public void setPartitionLeader(BrokerInfo partitionLeader) {
		this.partitionLeader = partitionLeader;
	}

	public List<BrokerInfo> getReplicas() {
		return replicas;
	}

	public void setReplicas(List<BrokerInfo> replicas) {
		this.replicas = replicas;
	}

	public List<BrokerInfo> getIsr() {
		return Isr;
	}

	public void setIsr(List<BrokerInfo> isr) {
		Isr = isr;
	}

	@Override
	public String toString() {
		return "PartitionInfo [partition=" + partition + ", partitionLeader=" + partitionLeader + ", replicas="
				+ replicas + ", Isr=" + Isr + "]";
	}
	
	
}
