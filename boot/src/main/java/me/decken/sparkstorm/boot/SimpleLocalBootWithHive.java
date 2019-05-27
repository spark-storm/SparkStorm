package me.decken.sparkstorm.boot;

/**
 * @author decken
 */
public class SimpleLocalBootWithHive extends BaseBoot {
    private String appName;

    public SimpleLocalBootWithHive(String appName) {
        this.appName = appName;
        init();
    }

    @Override public void option(SparkSessionBuilder builder) {
        builder.appName(appName).localMaster().enableHiveSupport();
    }
}
