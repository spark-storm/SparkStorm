package me.decken.sparkstorm.boot;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author decken
 */
public interface Boot extends Serializable {

    void create();

    SparkSession spark();

    SQLContext sqlContext();

    SparkContext sc();

    JavaSparkContext jsc();

    Dataset<Row> sql(String sqlString);
}
