package com.ingestion.dataset.writer;

import org.apache.avro.generic.GenericRecordBuilder;

import java.util.Objects;
import org.apache.avro.generic.GenericData.Record;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;

public class HiveDatasetWriterImpl implements HiveDatasetWriter {
	GenericRecordBuilder genericRecordBuilder=null;
	
	//try optional
	public void hiveDatasetParquetWriter(Dataset<Record> dataset, DatasetDescriptor datasetDescriptor, String[] data) {
		Objects.requireNonNull(datasetDescriptor, "Dataset Descriptor cannot be null");
		Record records = recordBuilder(datasetDescriptor, data);
		writeToHDFS(dataset, records);
	}
	
	private Record recordBuilder(DatasetDescriptor datasetDescriptor, String[] data){
		genericRecordBuilder = new GenericRecordBuilder(datasetDescriptor.getSchema());;
		Record record = genericRecordBuilder.set("username", "user-")
				.set("creationDate", System.currentTimeMillis()).set("favoriteColor", "").build();
		return record;
	}
	
	private void writeToHDFS(Dataset<Record> dataset, Record record){
		try (DatasetWriter<Record> datasetWriter = dataset.newWriter()) {
			//TO DO => add partition by
			datasetWriter.write(record);
		} catch (Exception excp) {
			excp.getMessage();
		}
	}
}
