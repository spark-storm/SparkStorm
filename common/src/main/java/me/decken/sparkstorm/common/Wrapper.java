package me.decken.sparkstorm.common;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.List;

import static me.decken.sparkstorm.common.util.CheckUtil.checkNotEmpty;

/**
 * @author decken
 */
public class Wrapper {


    /**
     * 将列名列表转成Column数组, 便于在一些可边长参数的api中使用
     *
     * @param colNameList
     * @return
     */
    public static Column[] toColumns(List<String> colNameList) {
        checkNotEmpty(colNameList, "colNameList不能为空");
        return colNameList.stream()
                .map(Column::new)
                .toArray(Column[]::new);
    }


    /**
     * 从dataset中获取schema信息
     *
     * @param dataset
     * @return
     */
    public static List<StructField> getFields(Dataset<Row> dataset) {
        return Lists.newArrayList(dataset.schema().fields());
    }
}
