package me.decken.sparkstorm.boot;

/**
 * @author decken
 */
public class SimpleLocalBoot extends BaseBoot {
    private String appName;

    public SimpleLocalBoot(String appName) {
        super();
        this.appName = appName;
        init();
    }

    @Override public void option(SparkSessionBuilder builder) {
        builder.appName(appName).localMaster();
    }
}
