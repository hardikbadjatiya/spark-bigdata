package com.amex.training.sparkcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest3 {

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
        JavaRDD<List<String>> rdd3=rdd1.map(line->Arrays.asList(line.split(" ")));
        rdd3.collect().forEach(list->System.out.println(list));
        
        
        

    }

}



  
  
  