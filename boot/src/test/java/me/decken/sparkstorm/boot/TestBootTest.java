package me.decken.sparkstorm.boot;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 * @author decken
 */
public class TestBootTest {

    @Test
    public void testCreate() {
        TestBoot boot = new TestBoot("test-boot");
        boot.create();
        boot.showConfig();

        Dataset<Row> df = boot.sqlContext().read().option("header", true).csv("file:///Users/decken/Documents/spark-storm/sparkstorm/data/users.txt");
        df.show();
        df.registerTempTable("abc");
        df.sqlContext().sql("select * from abc where id>2").show();
        df.write().saveAsTable("dd");
    }
}
