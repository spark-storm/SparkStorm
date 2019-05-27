package me.decken.sparkstorm.boot;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author decken
 */
public class BaseBootTest {
    private static final String APP_NAME = "test_boot";


    @Test
    public void localFileSystemTest() {
        BaseBoot boot = new BaseBoot() {
            @Override public void option(SparkSessionBuilder builder) {
                builder.appName(APP_NAME).localMaster().localFS();
            }
        };
        boot.init();
        String fs = boot.getConfig(BaseBoot.SparkSessionBuilder.DEFAULT_FS);
        assertNotNull(fs);
        System.out.println("fs:" + fs);
        Dataset<Row> data = boot.spark().read().csv("src/test/resources/data/users.txt");
        data.show();
        assertTrue(data.count() > 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void hdfsFileSystemTest() {
        BaseBoot boot = new BaseBoot() {
            @Override public void option(SparkSessionBuilder builder) {
                builder.appName(APP_NAME).localMaster().defaultFS("hdfs://notExistCluster");
            }
        };
        boot.init();
        Dataset<Row> data = boot.spark().read().load("abc");
        data.count();
    }

}
