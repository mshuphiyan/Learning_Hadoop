package com.learning.spark.writer;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


public class DefaultCSVToParquetImpl implements DefaultCSVToParquet, Serializable {
	private static final long serialVersionUID = 1L;
	SparkSession spark = null;

	public DefaultCSVToParquetImpl(SparkSession spark) {
		this.spark = spark;
	}

	public void csvToParquet(String csvFile, String parquetLocation) {
		Dataset<Row> df = spark.read().csv(csvFile);
		df.show();
		df.write().mode(SaveMode.Overwrite).parquet(parquetLocation);
	}

}
