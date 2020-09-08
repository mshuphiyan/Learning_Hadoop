package org.kitesdkexamples;

import java.util.Random;

import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configured;
import static org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;
import org.kitesdk.data.PartitionStrategy;

public class App extends Configured implements Tool {
	private static final String[] colors = { "green", "blue", "pink", "brown", "yellow" };

	public static void main(String[] args) {
		int rc = 0;
		try {
			rc = ToolRunner.run(new App(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(rc);
	}

	public int run(String[] arg0) throws Exception {
		PartitionStrategy partitionStrategy = new PartitionStrategy.Builder()
				.identity("favoriteColor", "favorite_color").build();

		DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schemaUri("resource:user.avsc")
				.format(Formats.PARQUET).partitionStrategy(partitionStrategy).build();

		//Dataset<Record> users = Datasets.create("dataset:hive?dataset=userparquet", descriptor, Record.class);//default location
		Dataset<Record> users = Datasets.create("dataset:hive:Data/Parquet/user3", descriptor, Record.class);
		DatasetWriter<Record> writer = null;
		try {
			writer = users.newWriter();
			Random rand = new Random();
			GenericRecordBuilder builder = new GenericRecordBuilder(descriptor.getSchema());
			for (int i = 0; i < 100; i++) {
				Record record = builder.set("username", "user-" + i).set("creationDate", System.currentTimeMillis())
						.set("favoriteColor", colors[rand.nextInt(colors.length)]).build();
				writer.write(record);
			}

		} finally {
			if (writer != null) {
				writer.close();
			}
		}

		return 0;
	}
}
