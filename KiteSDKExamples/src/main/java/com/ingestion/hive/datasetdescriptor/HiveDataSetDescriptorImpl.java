package com.ingestion.hive.datasetdescriptor;

import java.io.IOException;
import java.util.Objects;

import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;

public class HiveDataSetDescriptorImpl implements HiveDatasetDescriptor {
	public DatasetDescriptor getHiveDatasetDescriptor(String avrolocationuri, PartitionStrategy partitionStrategy) {
		Objects.requireNonNull(avrolocationuri, "avrolcoation uri cannot be null.");
		Objects.requireNonNull(partitionStrategy, "Partition Strategy cannot be null.");
		
		DatasetDescriptor descriptor = null;
		try {
			descriptor = new DatasetDescriptor.Builder().schemaUri(avrolocationuri).format(Formats.PARQUET)
					.partitionStrategy(partitionStrategy).build();
		} catch (IOException e) {
			e.getMessage();
		}
		return descriptor;
	}
}
