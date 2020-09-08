package com.learning.spark.writer;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DefaultParquetReaderImpl implements DefaultParquetReader, Serializable {
	private static final long serialVersionUID = 1L;

	SparkSession spark = null;

	public DefaultParquetReaderImpl(SparkSession spark) {
		this.spark = spark;
	}

	public void readParquet(String fileName) {
		Dataset<Row> parquetDF = spark.read().parquet(fileName);
		parquetDF.createOrReplaceTempView("parquetFile");

		Dataset<Row> recs = spark.sql("select * from parquetFile");
		Dataset<String> rec = recs.map(new MapFunction<Row, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Row row) throws Exception {
				return "First Col" + row.getString(1);
			}
		}, Encoders.STRING());

		rec.show();
	}

}
