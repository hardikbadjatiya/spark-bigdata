package com.amex.training.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSetWithUserDefinedSchema {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark=SparkSession.builder().appName("dataframe-test")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		Dataset<Row> empDS=spark.read().format("csv").option("header",true)
				.schema(employeeSchema())
				.load("c:/test/employee.csv");
		
		empDS.show();


	}
	
	static StructType employeeSchema()
	{
		return new StructType(
				new StructField[] {
				new StructField("emp_id", DataTypes.IntegerType,false,
						Metadata.empty()),
				new StructField("name", DataTypes.StringType,true,
						Metadata.empty()),
				new StructField("designation", DataTypes.StringType,false,
						Metadata.empty())
			
				});
	}

}
