package com.amex.training.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetTestWithDiffFormat {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark=SparkSession.builder().appName("dataframe-test")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		Dataset<Row> empDS=spark.read().format("csv").option("header",true)
				
				.load("c:/test/employee.csv");
		Dataset<Row> developers=empDS.where("designation='Developer'").select("id","name");
		developers.write().save("c:/developers");
		empDS.where("designation='Accountant'").select("id","name").write().format("csv")
				.option("header", true).save("c:/accountants");
		
		empDS.where("designation='Architect'").select("id","name").write().format("json").
		save("c:/architects");


	}

}
