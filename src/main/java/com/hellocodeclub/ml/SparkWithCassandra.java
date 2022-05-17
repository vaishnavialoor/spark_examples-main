package com.hellocodeclub.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.function.Function;
/**
 * Created by Subhankar on 12-05-2022.
 */
public class SparkWithCassandra {

    public static void main(String[] args) {

        /*SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "127.0.0.1");

        conf.setAppName("SparkWithCassandra");
        conf.setMaster("local[4]");*/
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.cassandra.connection.host", "127.0.0.1")
                .getOrCreate();
        //JavaSparkContext sc = new JavaSparkContext(conf);

        //Dataset<Row> readcsDF = spark.read().format("org.apache.spark.sql.cassandra").options(Map( "table" -> "asl", "keyspace" -> "January"));
        /*readcsDF.show();
        spark.stop();*/

        /*JavaRDD<String> name = javaFunctions(sc).cassandraTable("January", "asl",
                mapColumnTo(String.class)).
                select("name");*/

        Dataset<Row> readcsDF = spark.read().format("org.apache.spark.sql.cassandra")
                .options(new HashMap<String, String>() {
                    {
                        put("keyspace", "January");
                        put("table", "asl");
                    }
                }).load();
        readcsDF.show();

    }
}
