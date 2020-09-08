package com.ingestion.hive.datasetdescriptor;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;

public interface HiveDatasetDescriptor {
	public DatasetDescriptor getHiveDatasetDescriptor(String avrolocationuri, PartitionStrategy partitionStrategy);
}
