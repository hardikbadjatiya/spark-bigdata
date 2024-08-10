package com.amex.training.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark=SparkSession.builder().appName("dataframe-test")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		Dataset<Row> ds=spark.read().json("c:/test/users.json");
		ds.show();

	}

}
