package com.learning.spark.main;

import java.io.File;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		SparkSession spark = SparkSession.builder().appName(Main.class.getName())
				.config("spark.sql.warehouse.dir", warehouseLocation).master("local[*]").enableHiveSupport()
				.getOrCreate();
		Dataset<Row> row= spark.sql("select * from parquet.user2");
	}
}