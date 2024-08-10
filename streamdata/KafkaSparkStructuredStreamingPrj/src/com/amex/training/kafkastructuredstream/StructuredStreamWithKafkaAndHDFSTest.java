package com.amex.training.kafkastructuredstream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;

import static org.apache.spark.sql.functions.*;

import java.util.concurrent.TimeoutException;
public class StructuredStreamWithKafkaAndHDFSTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark=SparkSession.builder().appName("kafka-structured-stream-test")
				.master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		
		Dataset<Row> df=spark.readStream().format("kafka")
						.option("kafka.bootstrap.servers", "localhost:9092")
						.option("subscribe", "second-topic")
						.load()
						.select(col("value").cast("string"));

		/*Dataset<Row> wordCount=
				df.select(explode(split(col("value")," ")).alias("words"))
				.groupBy("words").count();*/
		try {
			StreamingQuery query= df.writeStream()
			//.outputMode(OutputMode.Complete())
			.format("json")
			.option("checkpointLocation","c:/wordcountchkpoint")
			.start("hdfs://localhost:9820/training/jsonwordcount");
			
			
			System.out.println("streaming started");
			Thread.sleep(10*60*1000);
			query.stop();
			
			
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
