package me.decken.sparkstorm.boot.component;

import me.decken.sparkstorm.boot.Boot;
import me.decken.sparkstorm.boot.common.TestBootWithHive;
import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author decken
 */
public class HiveTableTest {

    Boot boot = new TestBootWithHive();

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

}