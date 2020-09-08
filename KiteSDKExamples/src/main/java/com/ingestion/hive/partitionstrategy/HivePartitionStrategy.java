package com.ingestion.hive.partitionstrategy;

import org.kitesdk.data.PartitionStrategy;

public interface HivePartitionStrategy {
	public PartitionStrategy getPartitionStrategy(String colname);
}
