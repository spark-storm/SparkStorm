package me.decken.sparkstorm.boot;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveUtils;
import org.apache.spark.sql.internal.SQLConf;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static me.decken.sparkstorm.common.util.CheckUtil.checkNotBlank;
import static me.decken.sparkstorm.common.util.CollectionUtil.mapToJavaMap;
import static me.decken.sparkstorm.common.util.FormatUtil.format;
import static me.decken.sparkstorm.common.util.FormatUtil.mapToKvString;

/**
 * @author decken
 */
@Slf4j
public abstract class BaseBoot {

    @Getter
    private SparkSession session;

    public BaseBoot() {
        SparkSessionBuilder builder = new SparkSessionBuilder(SparkSession.builder());
        // 默认开启的选项, 最后以option里面的选项为准
        defaultBuiler(builder);
        option(builder);
        this.session = builder.sessionBuilder.getOrCreate();
    }

    private SparkSessionBuilder defaultBuiler(SparkSessionBuilder builder) {
        builder.defaultSqlConfig();
        builder.kryoSerializer();   // 默认使用kryo
        builder.orcHive();
        return builder;
    }

    /**
     * 子类实现该类选择需要打开的选项
     *
     * @param builder
     */
    public abstract void option(SparkSessionBuilder builder);

    /**
     * @return sparkSession对象
     */
    public SparkSession spark() {
        return this.session;
    }

    /**
     * 保留这个是为了兼容1.x之前的接口, 不建议使用了,建议直接使用 [[SparkContext]] 或者 [[JavaSparkContext]]
     *
     * @return SQLContext对象
     */
    @Deprecated
    public SQLContext sqlContext() {
        return this.session.sqlContext();
    }

    /**
     * @return scala版本的SparkContext
     */
    public SparkContext sc() {
        return session.sparkContext();
    }

    /**
     * 兼容java的SparkContext
     *
     * @return
     */
    public JavaSparkContext jsc() {
        return new JavaSparkContext(sc());
    }

    /**
     * 执行sql返回Dataset<Row>
     *
     * @param sqlString
     * @return
     */

    public Dataset<Row> sql(String sqlString) {
        return this.session.sql(sqlString);
    }


    public void debug() {
        log.info("WebUI url:{}", webUIUrl());
        keep();
    }

    /**
     * 保持Spark不关闭, 调试的时候非常有用
     */
    public void keep() {
        // 1小时
        keep(60 * 60);
    }

    public void keep(Integer second) {
        try {
            Thread.sleep(second * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String webUIUrl() {
        return sc().uiWebUrl().get();
    }

    /**
     * 打印所有的配置
     */
    public void showConfig() {
        Map<String, String> configMap = getAllConfig();
        log.info("all config:\n{}", mapToKvString(configMap));
    }

    public Map<String, String> getAllConfig() {
        return mapToJavaMap(spark().conf().getAll());
    }

    public String getConfig(String key) {
        return getAllConfig().get(key);
    }

    public Map<String, String> getSqlConfig() {
        return mapToJavaMap(spark().conf().getAll());
    }


    /**
     * 构建SparkSession.Builder的辅助类
     */
    public static class SparkSessionBuilder {

        public static final String DEFAULT_FS = "fs.defaultFS";

        public static final String SERIALIZER = "spark.serializer";

        protected SparkSession.Builder sessionBuilder;

        private List<KryoRegistrator> kryoRegistrators;

        public SparkSessionBuilder(SparkSession.Builder sessionBuilder) {
            this.sessionBuilder = sessionBuilder;
        }


        public SparkSessionBuilder appName(String appName) {
            checkNotBlank(appName, "应用名不能为空");
            sessionBuilder.appName(appName);
            return this;
        }

        public SparkSessionBuilder kryoRegistrator(List<KryoRegistrator> kryoRegistrator) {
            if (kryoRegistrator == null) {
                return this;
            }

            if (this.kryoRegistrators == null) {
                this.kryoRegistrators = Lists.newArrayList();
            }
            this.kryoRegistrators.addAll(kryoRegistrator);

            if (!this.kryoRegistrators.isEmpty()) {
                List<String> clsNames = kryoRegistrators.stream()
                        .map(item -> item.getClass().getName())
                        .filter(String::isEmpty)
                        .collect(Collectors.toList());
                sessionBuilder.config("spark.kryo.registrator", StringUtils.join(clsNames, ","));
            }
            return this;
        }


        public SparkSessionBuilder kryoRegistrator(KryoRegistrator kryoRegistrator) {
            if (kryoRegistrator != null) {
                kryoRegistrator(Lists.newArrayList(kryoRegistrator));
            }
            return this;
        }

        public SparkSessionBuilder kryoSerializer() {
            sessionBuilder.config(SERIALIZER, KryoSerializer.class.getName());
            sessionBuilder.config("spark.kryoserializer.buffer.max", "256M");
            sessionBuilder.config("spark.kryo.unsafe", true);
            return this;
        }

        /**
         * 开启kryoSerializer的情况下, 如果需要序列化的类没有在kryoRegistrator注册, 则序列化的时候会在每个对象的加上全类名, 会严重影响性能.
         * 开启这个选项在遇到没有注册的类的时候会抛出异常, 是否开启就要你在性能和编码灵活性上取舍了.
         *
         * @return
         */
        public SparkSessionBuilder KryoRegistrationRequired() {
            sessionBuilder.config("spark.kryo.registrationRequired", true);
            return this;
        }

        public SparkSessionBuilder javaSerializer() {
            sessionBuilder.config(SERIALIZER, JavaSerializer.class.getName());
            return this;
        }

        public SparkSessionBuilder enableHiveSupport() {
            sessionBuilder.enableHiveSupport()
                    //动态分区特性
                    .config("hive.exec.dynamic.partition", "true")
                    .config("hive.exec.dynamic.partition.mode", "nonstrict");
            return this;
        }

        public SparkSessionBuilder orcHive() {
            sessionBuilder.config(HiveUtils.CONVERT_METASTORE_ORC().key(), true);
            sessionBuilder.config(SQLConf.HIVE_CASE_SENSITIVE_INFERENCE().key(), "NEVER_INFER");
            sessionBuilder.config(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED().key(), true);
//            sessionBuilder.config(SQLConf.ORC_COMPRESSION().key(), "snappy");
            return this;
        }

        public SparkSessionBuilder disableOrcHive() {

            sessionBuilder.config(HiveUtils.CONVERT_METASTORE_ORC().key(), Boolean.valueOf(HiveUtils.CONVERT_METASTORE_ORC().defaultValueString()));
            sessionBuilder.config(SQLConf.HIVE_CASE_SENSITIVE_INFERENCE().key(), SQLConf.HIVE_CASE_SENSITIVE_INFERENCE().defaultValueString());
            sessionBuilder.config(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED().key(), Boolean.valueOf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED().defaultValueString()));
            return this;
        }

        public SparkSessionBuilder config(String key, String value) {
            checkNotBlank(key, "key不能为空");
            sessionBuilder.config(key, value);
            return this;
        }

        public SparkSessionBuilder localMaster() {
            sessionBuilder.master("local[1]");
            return this;
        }

        public SparkSessionBuilder localMaster(Integer numThread) {
            checkArgument(numThread > 0, "");
            sessionBuilder.master(format("local[{}]", numThread));
            return this;
        }

        public SparkSessionBuilder yarnMaster() {
            sessionBuilder.master("yarn");
            return this;
        }

        public SparkSessionBuilder defaultFS(String defaultFS) {
            this.sessionBuilder.config(DEFAULT_FS, defaultFS);
            return this;
        }

        /**
         * 本地文件系统作为hdfs读写的路径, 主要用于测试
         *
         * @return
         */
        public SparkSessionBuilder localFS() {
            this.sessionBuilder.config(DEFAULT_FS, "file:///");
            return this;
        }

        public SparkSessionBuilder master(String master) {
            checkNotBlank(master, "master不能为空");
            sessionBuilder.master(master);
            return this;
        }


        public SparkSessionBuilder config(SparkConf config) {
            sessionBuilder.config(config);
            return this;
        }

        public SparkSessionBuilder defaultSqlConfig() {
            this.sessionBuilder.config(SQLConf.CROSS_JOINS_ENABLED().key(), "true");
            return this;
        }
    }

}
