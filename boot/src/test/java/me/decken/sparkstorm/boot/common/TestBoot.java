package me.decken.sparkstorm.boot.common;

import me.decken.sparkstorm.boot.Boot;

/**
 * @author decken
 */
public class TestBoot extends Boot {
    @Override public void option(SparkSessionBuilder builder) {
        builder.appName("spark_test").localMaster();
    }
}
