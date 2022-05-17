package com.hellocodeclub.ml;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Created by Subhankar on 13-05-2022.
 */
public class ReadParquetAndSparkSQL {
    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ReadParquetAndSparkSQL")
                .config("spark.master", "local")
                .getOrCreate();

       // Dataset<Row> peopleDF = spark.read().json("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\people.json");

// DataFrames can be saved as Parquet files, maintaining the schema information
        //peopleDF.write().parquet("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\people.parquet");

        // Read in the Parquet file created above.
// Parquet files are self-describing so the schema is preserved
// The result of loading a parquet file is also a DataFrame
        Dataset<Row> parquetFileDF = spark.read().parquet("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\userdata1.parquet");

        parquetFileDF.show();

        parquetFileDF.drop(col("title")).printSchema();
// Parquet files can also be used to create a temporary view and then used in SQL statements
       // parquetFileDF.createOrReplaceTempView("parquetFile");
        /*Dataset<Row> namesDF = spark.sql("SELECT first_name FROM parquetFile WHERE gender is Male");
        namesDF.show();*/


        /*Dataset<Row> parquetFileDF2 = parquetFileDF.withColumn("new_gender", when(col("gender") === "MALE","M")
                .when(col("gender") === "Female","F")
                .otherwise("Unknown"));


        Dataset<Row> parquetFileDF2 = parquetFileDF.withColumn("new_gender",
                expr("case when gender = 'M' then 'Male' " +
                        "when gender = 'F' then 'Female' " +
                        "else 'Unknown' end"))*/

       Dataset<Row> parquetFileDF2 = parquetFileDF.withColumn("Date_Of_Birth",add_months(col("birthdate"),2));
       parquetFileDF2.show();
        parquetFileDF2.printSchema();
// +------------+
// |       value|
// +------------+
// |Name: Justin|
// +------------+
    }
}
