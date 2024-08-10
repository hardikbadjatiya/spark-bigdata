package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDFlatMapTest {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf conf=new SparkConf();
        conf.setAppName("rdd-flat-map-test");
        conf.setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        
        JavaRDD<String> rdd1=sc.textFile("c:/test/first.txt");
        JavaRDD<String> rdd2=rdd1.repartition(4);
        System.out.println("Total number of partitions: "+rdd2.getNumPartitions());
        rdd1.flatMap(line->Arrays.asList(line.split(" ")).iterator())
        .filter(word->word.length()>4).map(word->word.toUpperCase()).
        repartition(1).saveAsTextFile("c:/testout1");
        

    }

}



  
  
  