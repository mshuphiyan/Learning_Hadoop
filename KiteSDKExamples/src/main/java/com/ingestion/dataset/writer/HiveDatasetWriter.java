package com.ingestion.dataset.writer;

import org.apache.avro.generic.GenericData.Record;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;

public interface HiveDatasetWriter {
	public void hiveDatasetParquetWriter(Dataset<Record> dataset, DatasetDescriptor datasetDescriptor, String[] data);
}
