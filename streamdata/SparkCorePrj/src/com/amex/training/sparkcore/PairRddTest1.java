package com.amex.training.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRddTest1 {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf conf=new SparkConf();
        conf.setAppName("pair-rdd-creation-test");
        conf.setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        
        JavaRDD<String> rdd1=sc.textFile("c:/test/users.tsv");
        JavaPairRDD<String, String> rdd2=rdd1.map(line->line.split("\t"))
        		.mapToPair(arr->new Tuple2<String,String>(arr[0], arr[1]));
        rdd2.collect().forEach(t->System.out.println("key: "+t._1+" value: "+t._2));
    }

}



  
  
  