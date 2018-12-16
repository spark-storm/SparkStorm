package me.decken.sparkstorm.boot;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

import java.util.List;
import java.util.Map;

import static me.decken.sparkstorm.common.CollectionUtil.mapToJavaMap;
import static me.decken.sparkstorm.common.FormatUtil.mapToKv;

/**
 * @author decken
 */
@Slf4j
public abstract class AbstractBoot implements Boot {
    private SparkSession session;

    private String appName;

    @Setter @Getter private List<KryoRegistrator> kryoRegistrator;

    private SparkSession.Builder builder;

    @Setter @Getter private SparkConf config;

    @Setter @Getter private Boolean enableHive = false;

    public AbstractBoot(String appName) {
        this.appName = appName;
    }

    public void enableHive() {
        enableHive = true;
    }

    @Override public SparkSession spark() {
        return this.session;
    }

    @Override public SQLContext sqlContext() {
        return this.session.sqlContext();
    }

    @Override public SparkContext sc() {
        return session.sparkContext();
    }

    @Override public JavaSparkContext jsc() {
        return new JavaSparkContext(sc());
    }

    @Override
    public void create() {
        log.info("开始启动应用:{}", appName);
        buildSparkSession();
        this.session = builder.getOrCreate();

        log.info("appName:{} get session finished", appName);
    }


    public void buildSparkSession() {
        String master = "local[4]";
        this.builder = SparkSession.builder();
        if (StringUtils.isNotBlank(appName)) {
            builder.appName(appName);
        }

        builder.master(master);

//        setHdfsConfig();
        setKryoRegistrator();
        setHiveSupport();
        setSqlConfig();

        if (this.config != null) {
            // 以用户传递的配置进行覆盖
            builder.config(config);
        }

    }


    /**
     * 打印所有的配置
     */
    public void showConfig() {
        Map<String, String> configMap = getAllConfig();
        log.info("all config:\n{}", mapToKv(configMap));
    }

    public Map<String, String> getAllConfig() {
        return mapToJavaMap(spark().conf().getAll());
    }

    public Map<String, String> getSqlConfig() {
        return mapToJavaMap(sqlContext().conf().getAllConfs());
    }

    protected void setHdfsConfig() {
        String hdfsCluster = "hdfs://blackstone190061:9000";
        this.builder.config("fs.defaultFS", hdfsCluster);
    }

    protected void setSqlConfig() {
        this.builder.config(SQLConf.CROSS_JOINS_ENABLED().key(), "true");
    }

    protected void setKryoRegistrator() {
        if (kryoRegistrator != null && !kryoRegistrator.isEmpty()) {
            builder.config("spark.kryo.registrator", kryoRegistrator.getClass().getName());
        }
    }

    protected void setHiveSupport() {
        if (enableHive) {
            builder.enableHiveSupport()
                    //动态分区特性
                    .config("hive.exec.dynamic.partition", "true")
                    .config("hive.exec.dynamic.partition.mode", "nonstrict");
        }
    }
}
