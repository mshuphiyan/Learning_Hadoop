package com.ingestion.engine.main;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdkexamples.App;

import com.ingestion.dataset.writer.HiveDatasetWriter;
import com.ingestion.dataset.writer.HiveDatasetWriterImpl;
import com.ingestion.hive.dataset.HiveDataset;
import com.ingestion.hive.dataset.HiveDatasetImpl;
import com.ingestion.hive.datasetdescriptor.HiveDataSetDescriptorImpl;
import com.ingestion.hive.datasetdescriptor.HiveDatasetDescriptor;
import com.ingestion.hive.partitionstrategy.HivePartitionStrategy;
import com.ingestion.hive.partitionstrategy.HivePartitionStrategyImpl;

public class MainIngestion extends Configured implements Tool {

	public static void main(String[] args) {
		int rc = 0;
		try {
			rc = ToolRunner.run(new App(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(rc);
	}

	// convert into spring project
	public int run(String[] a) throws Exception {
		String[] data = null;
		// 1. partition strategy
		HivePartitionStrategy hivePartitionStrategy = new HivePartitionStrategyImpl();
		//PartitionStrategy partitionStrategy = hivePartitionStrategy.getPartitionStrategy("");

		// 2. dataset descriptor
		HiveDatasetDescriptor hiveDatasetDescriptor = new HiveDataSetDescriptorImpl();
		DatasetDescriptor descriptor = hiveDatasetDescriptor.getHiveDatasetDescriptor("", hivePartitionStrategy.getPartitionStrategy(""));

		// 3. record and hive details
		HiveDataset hiveDataset = new HiveDatasetImpl();
		//Dataset<Record> dataset = hiveDataset.getHiveDataset("", descriptor);

		// 4. dataset writer
		HiveDatasetWriter hiveDatasetWriter = new HiveDatasetWriterImpl();
		hiveDatasetWriter.hiveDatasetParquetWriter(hiveDataset.getHiveDataset("", descriptor), descriptor, data);

		return 0;
	}

}
