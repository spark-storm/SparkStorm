package me.decken.sparkstorm.common;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * @author xuande
 */
public class SparkUtil implements Serializable {
    private static final long serialVersionUID = 5346664404899760705L;
    public static List<DataType> baseDataType = Lists.newArrayList(StringType, BinaryType, BooleanType, DateType,
            TimestampType, CalendarIntervalType, DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, NullType);
    public static List<DataType> arrayDataType = baseDataType.stream().map(i -> DataTypes.createArrayType(i)).collect(Collectors.toList());


    public static UDFRegistration udf(Dataset<Row> dataset) {
        return dataset.sqlContext().udf();
    }

    public static void registerJavaUdf(Dataset<Row> dataset, String name, String className, DataType returnType) {
        dataset.sqlContext().udf().registerJava(name, className, returnType);
    }

    public static void registerJavaUdf(SQLContext sqlContext, String name, String className, DataType returnType) {
        sqlContext.udf().registerJava(name, className, returnType);
    }

    public static String getSparkUIUrl(SparkContext sparkContext) {
        checkNotNull(sparkContext, "sparkContext不能为null");
        SparkConf conf = sparkContext.getConf();
        String urls = conf.get("spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASES", "");
        String url;
        if (StringUtils.isNotBlank(urls)) {
            url = StringUtils.split(urls, ",")[0];
        } else {
            String host = conf.get("spark.driver.host", "");
            String port = conf.get("spark.driver.port", "");
            url = String.format("http://%s:%s/jobs/", host, port);
        }
        return url;
    }

    public static String getSparkUIUrl(Dataset<Row> dataset) {
        return getSparkUIUrl(dataset.sparkSession().sparkContext());
    }

    public static void setJobDescription(SparkSession sparkSession, String description) {
        sparkSession.sparkContext().setJobDescription(description);
    }

    public static void clearJobDescription(SparkSession sparkSession) {
        sparkSession.sparkContext().setJobDescription(null);
    }

    public static Boolean isFieldExist(Dataset<Row> df, final String field) {
        try {
            df.schema().fieldIndex(field);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }


    public static String prettySchema(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        return prettySchema(schema);
    }

    public static String prettySchema(StructType schema) {
        List<String> fList = new ArrayList<>();
        for (StructField field : schema.fields()) {
            fList.add(field.name() + ":" + field.dataType());
        }
        return StringUtils.join(fList, " ");
    }
}
