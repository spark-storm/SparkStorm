package me.decken.sparkstorm.boot;

/**
 * @author decken
 */
public class SimpleBootWithHive extends BaseBoot {
    private String appName;

    public SimpleBootWithHive(String appName) {
        this.appName = appName;
    }

    @Override public void option(SparkSessionBuilder builder) {
        builder.appName(appName).localMaster().enableHiveSupport();
    }
}
