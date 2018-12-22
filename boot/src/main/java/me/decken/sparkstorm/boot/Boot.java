package me.decken.sparkstorm.boot;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
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
public abstract class Boot {

    @Getter
    private SparkSession session;

    public Boot() {
        SparkSessionBuilder builder = new SparkSessionBuilder(SparkSession.builder());
        option(builder);
        this.session = builder.sessionBuilder.getOrCreate();
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

    public Map<String, String> getSqlConfig() {
        return mapToJavaMap(spark().conf().getAll());
    }


    /**
     * 构建SparkSession.Builder的辅助类
     */
    public static class SparkSessionBuilder {

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

        public SparkSessionBuilder enableHiveSupport() {
            sessionBuilder.enableHiveSupport()
                    //动态分区特性
                    .config("hive.exec.dynamic.partition", "true")
                    .config("hive.exec.dynamic.partition.mode", "nonstrict");
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
            this.sessionBuilder.config("fs.defaultFS", defaultFS);
            return this;
        }

        public SparkSessionBuilder localFS(String path) {
            this.sessionBuilder.config("fs.defaultFS", format("file://{}", path));
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
