package me.decken.sparkstorm.boot.component;

import me.decken.sparkstorm.boot.BaseBoot;
import me.decken.sparkstorm.boot.common.TestBootWithHive;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static me.decken.sparkstorm.common.util.JarUtil.getJarFileAbsPath;

/**
 * @author decken
 */
public class HiveTableTest {

    private BaseBoot boot = new TestBootWithHive();

    @Test
    public void createSinglePartitionTable() {
        HiveTable table = HiveTable.builder().tableName("single_partition_table")
                .partitionName(HiveTable.DEFAULT_PARTITION_NAME)
                .sparkSession(boot.spark())
                .build();
        assertTrue(table.getSinglePartitionTable());
        assertTrue(table.getPartitionTable());
    }

    @Test
    public void createCommonTable() {
        HiveTable table = HiveTable.builder().tableName("common_table")
                .sparkSession(boot.spark())
                .build();
        assertFalse(table.getSinglePartitionTable());
        assertFalse(table.getPartitionTable());
    }


    @Test
    public void saveData() {
        Dataset<Row> df = getData();
        HiveTable table = new HiveTable(boot.spark(), "test_data_common");
        table.save(df);
    }


    protected Dataset<Row> getData() {
        String path = getJarFileAbsPath("data/users.txt");

        Dataset<Row> df = boot.sqlContext()
                .read()
                // 具体有哪些可选项, 可以看org.apache.spark.sql.DataFrameReader#csv上的注释, 或者直接看实现csv作为数据源的模块中org.apache.spark.sql.execution.datasources.csv.CSVOptions
                .option("header", true)
                .option("inferSchema", "true")
                .csv(path);
        return df;
    }


}