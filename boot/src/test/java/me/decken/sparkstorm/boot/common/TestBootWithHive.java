package me.decken.sparkstorm.boot.common;

import me.decken.sparkstorm.boot.BaseBoot;

/**
 * @author decken
 */
public class TestBootWithHive extends BaseBoot {

    public TestBootWithHive() {
        init();
    }

    @Override public void option(SparkSessionBuilder builder) {
        builder.appName("spark_test").localMaster().enableHiveSupport();
    }
}
