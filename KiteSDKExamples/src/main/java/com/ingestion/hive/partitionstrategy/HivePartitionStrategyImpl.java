package com.ingestion.hive.partitionstrategy;

import java.util.Objects;
import org.kitesdk.data.PartitionStrategy;

public class HivePartitionStrategyImpl implements HivePartitionStrategy {

	public PartitionStrategy getPartitionStrategy(String colname) {
		Objects.requireNonNull(colname, "partition column should ne empty");

		PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().identity(colname, colname).build();
		return partitionStrategy;
	}

}
