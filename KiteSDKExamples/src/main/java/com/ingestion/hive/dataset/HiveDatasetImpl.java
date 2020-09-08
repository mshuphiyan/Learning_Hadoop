package com.ingestion.hive.dataset;

import java.util.Objects;

import org.apache.avro.generic.GenericData.Record;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.Datasets;

public class HiveDatasetImpl implements HiveDataset {

	public Dataset<Record> getHiveDataset(String parquetLocation, DatasetDescriptor datasetDescriptor) {
		Objects.requireNonNull(parquetLocation, "Define parquet location");
		Objects.requireNonNull(datasetDescriptor, "Dataset descriptor cannot be null");

		return Datasets.create(parquetLocation, datasetDescriptor, Record.class);
	}

}
