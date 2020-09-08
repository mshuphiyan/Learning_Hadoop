package com.ingestion.hive.dataset;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;

import static org.apache.avro.generic.GenericData.Record;

public interface HiveDataset {
	public Dataset<Record> getHiveDataset(String parquetLocation, DatasetDescriptor datasetDescriptor);
}
