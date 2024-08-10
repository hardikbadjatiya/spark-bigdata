package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTest2 {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf conf=new SparkConf();
        conf.setAppName("rdd-map-test");
        conf.setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        
        JavaRDD<Integer> rdd1=sc.parallelize(Arrays.asList(3,6,7,9));
        rdd1.map(n->n*2).map(n->n*n).map(n->n+1).foreach(n->System.out.println(n));

    }

}



  
  
  