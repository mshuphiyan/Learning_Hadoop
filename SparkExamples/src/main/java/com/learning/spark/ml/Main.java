package com.learning.spark.ml;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.Metadata;

public class Main {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName(Main.class.getName()).master("local[*]").getOrCreate();
		List<Row> dataTraining = Arrays.asList(RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
				RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
				RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
				RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5)));
		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("features", new VectorUDT(), false, Metadata.empty()) });

		Dataset<Row> training = spark.createDataFrame(dataTraining, schema);

		LogisticRegression lr = new LogisticRegression();

		System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");
		lr.setMaxIter(10).setRegParam(0.01);

		LogisticRegressionModel model1 = lr.fit(training);
		System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

		ParamMap paramMap = new ParamMap().put(lr.maxIter().w(20)).put(lr.maxIter(), 30).put(lr.regParam().w(0.1),
				lr.threshold().w(0.55));

		// One can also combine ParamMaps.
		ParamMap paramMap2 = new ParamMap().put(lr.probabilityCol().w("myProbability"));

		ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);
	}

}
