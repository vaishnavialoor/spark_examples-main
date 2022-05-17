package com.hellocodeclub.ml;

/**
 * Created by Subhankar on 11-05-2022.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
public class WordCounter {

    private static void wordCount(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        //JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")));

       // JavaPairRDD countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        JavaPairRDD<String, Integer> counts = inputFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
       // counts.saveAsTextFile("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\CountData.txt");
    System.out.println(counts.collect());
    }

    public static void main(String[] args) {



        wordCount("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\input.txt");
    }
}