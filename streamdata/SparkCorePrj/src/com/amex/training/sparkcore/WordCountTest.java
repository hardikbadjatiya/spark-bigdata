package com.amex.training.sparkcore;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountTest {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        SparkConf conf=new SparkConf();
        conf.setAppName("wordcount-test");
        conf.setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        
        JavaRDD<String> rdd1=sc.textFile("c:/test/words.txt");
        JavaPairRDD<String, Integer> rdd2=rdd1.flatMap(line->Arrays.asList(line.split(" ")).iterator())
        		.mapToPair(word->new Tuple2<String,Integer>(word, 1));
        rdd2.reduceByKey((x,y)->x+y).collect().forEach(t->System.out.println("word: "+t._1+
        		" no-of-occurrences: "+t._2));
    }

}



  
  
  