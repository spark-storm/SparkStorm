package me.decken.sparkstorm.boot;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import static me.decken.sparkstorm.common.JarUtil.getJarFileAbsPath;

/**
 * @author decken
 */
public class TestBootTest {

    @Test
    public void testCreate() {
        TestBoot boot = new TestBoot("test-boot");
        boot.create();
        boot.showConfig();
        String path = getJarFileAbsPath("data/users.txt");

        Dataset<Row> df = boot.sqlContext()
                .read()
                // 具体有哪些可选项, 可以看org.apache.spark.sql.DataFrameReader#csv上的注释, 或者直接看实现csv作为数据源的模块中org.apache.spark.sql.execution.datasources.csv.CSVOptions
                .option("header", true)
                .option("inferSchema", "true")

                .csv(path);

        df.printSchema();
        df.show();
        df.registerTempTable("abc");
        df.sqlContext().sql("select * from abc where id>2").show();
        df.write().mode(SaveMode.Overwrite).saveAsTable("dd");
    }
}
