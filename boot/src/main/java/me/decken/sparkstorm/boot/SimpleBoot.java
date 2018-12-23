package me.decken.sparkstorm.boot;

/**
 * @author decken
 */
public class SimpleBoot extends BaseBoot {
    private String appName;

    public SimpleBoot(String appName) {
        this.appName = appName;
    }

    @Override public void option(SparkSessionBuilder builder) {
        builder.appName(appName).localMaster();
    }
}
