package me.decken.sparkstorm.boot;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.hive.orc.OrcFileFormat;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static me.decken.sparkstorm.common.SparkUtil.clearJobDescription;
import static me.decken.sparkstorm.common.SparkUtil.setJobDescription;
import static me.decken.sparkstorm.common.Wrapper.toColumns;
import static me.decken.sparkstorm.common.util.CheckUtil.*;
import static me.decken.sparkstorm.common.util.CollectionUtil.javaListToSeq;
import static me.decken.sparkstorm.common.util.FormatUtil.format;

/**
 * @author decken
 */
@Slf4j
public class HiveTable implements Serializable {
    private static final long serialVersionUID = 635439231994501928L;
    private static final String DEFAULT_PARTITION_NAME = "visit_date";

    /**
     * 默认是orc格式的hive表
     */

    private static DataSourceRegister dataSourceRegister = new OrcFileFormat();
    private String tableName;
    private List<String> partitionNames;
    private Boolean partitionTable = false;
    private Boolean singlePartitionTable = false;
    private SparkSession sparkSession;


    public HiveTable(SparkSession sparkSession, String tableName) {
        this(sparkSession, tableName, Lists.newArrayList());
    }

    public HiveTable(SparkSession sparkSession, String tableName, String partitionName) {
        this(sparkSession, tableName, Lists.newArrayList(partitionName));

    }

    public HiveTable(SparkSession sparkSession, String tableName, List<String> partitionNames) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.sparkSession = sparkSession;
        this.partitionTable = true;

        if (partitionNames.size() == 1) {
            this.singlePartitionTable = true;
        }

        checkArgument();
    }

    private void checkArgument() {
        checkNotBlank(tableName, "表名不能为空");
        checkNotNull(sparkSession, "sparkSession不能为null");
        if (partitionTable) {
            checkNotEmpty(partitionNames, "分区表的分区字段不能为空");
        }

        if (singlePartitionTable) {
            Preconditions.checkArgument(partitionNames.size() == 1);
        }
    }


    /**
     * 实现根据实际的数据修改表结构,在表的后面append新列
     *
     * @param data
     */
    public void alterTableSchema(final Dataset<Row> data) {
        TableIdentifier identifier = null;
        identifier = getTableIdentifier();
        SessionCatalog cateLog = getCatalog();
        List<String> tableCols = getTableColName();

        if (isLessField(data)) {
            throw new RuntimeException(format("数据列比表[{}]少, 数据的列:{}, 表的列:{}", this.tableName, getDatasetFields(data), tableCols));
        }
        if (isFieldSame(data)) {
            log.info("表[{}]字段和数据一致, 无需修改", this.tableName);
            return;
        } else {
            log.info("修改表[{}]的schema, 原表字段:{} 修改后:{}", this.tableName, tableCols, getDatasetFields(data));
            Dataset<Row> newData = rearrange(data);
            for (String n : partitionNames) {
                newData = newData.drop(n);
            }

            // 不能修改partition的schema
            cateLog.alterTableDataSchema(identifier, newData.schema());
            cateLog.refreshTable(identifier);
        }
    }


    /**
     * 按照现有表的顺序重排, 如果data中有新列, 则追加到后面
     *
     * @param data
     * @return
     */
    protected Dataset<Row> rearrange(Dataset<Row> data) {
        List<String> tableCols = getTableColName();

        if (isLessField(data)) {
            throw new RuntimeException(format("数据字段缺失, 表[{}]字段为:{}, data字段为:{}",
                    tableName, tableCols, getDatasetFields(data)));
        }

        List<String> newCols = findNewFields(data);
        if (!newCols.isEmpty()) {
            for (String newCol : newCols) {
                if (!tableCols.contains(newCol)) {
                    tableCols.add(newCol);
                }
            }
        }
        log.info("原始字段:{} 重排为:{}", getDatasetFields(data), tableCols);
        data = data.select(toColumns(tableCols));
        return data;
    }


    /**
     * @param data
     */
    public void save(Dataset<Row> data) {
        if (partitionTable) {
            save(data, null, false, false);
        } else {
            saveCommonTable(data);
        }
    }

    public void save(Dataset<Row> data, String partition) {
        save(data, partition, false, false);
    }

    public void save(Dataset<Row> data, String partition, Boolean isAppendSchema) {
        save(data, partition, isAppendSchema, false);
    }

    public void sava(Dataset<Row> data, Boolean isAppendSchema, Boolean isOverwrite) {
        save(data, null, isAppendSchema, isOverwrite);
    }

    /**
     * 保存分区表
     *
     * @param data
     */
    public void save(final Dataset<Row> data, Boolean isAppendSchema) {
        save(data, null, isAppendSchema, false);
    }


    /**
     * 保存分区表
     *
     * @param data
     * @param partition
     * @param isAppendSchema
     * @param isOverwrite
     */
    public void save(final Dataset<Row> data, String partition, Boolean isAppendSchema, Boolean isOverwrite) {
        checkIsPartitionTable();
        Dataset<Row> newData = data;
        SaveMode saveMode = SaveMode.Overwrite;

        if (singlePartitionTable && StringUtils.isNotBlank(partition)) {
            // 单分区表且指定了分区的情况下强制指定分区列
            newData = newData.withColumn(getPartitionName(), functions.lit(partition));
        }

        if (isOverwrite) {
            log.info("开始覆盖写分区表table:{}", this.tableName);
        } else {
            if (isTableExist()) {
                //TODO: 检查相同字段的类型是否相同
                newData = rearrange(newData);
                saveMode = SaveMode.Append;
                if (isAppendSchema) {
                    alterTableSchema(newData);
                }
            }
            log.info("开始写分区表:{}", this.tableName);
        }
        setJobDescription(sparkSession, format("保存分区表:{}", tableName));
        newData.write()
                .mode(saveMode)
                .format(dataSourceRegister.shortName())
                .partitionBy(partitionNames.toArray(new String[0]))
                .saveAsTable(tableName);
        clearJobDescription(sparkSession);
        log.info("保存分区表:{} 已经写完", this.tableName);
    }


    /**
     * 保存一张普通表
     *
     * @param df
     */
    private void saveCommonTable(Dataset<Row> df) {
        if (partitionTable) {
            throw new RuntimeException(format("表:{}是分区表", tableName));
        }
        log.info("开始写表:{}", this.tableName);
        setJobDescription(sparkSession, format("保存分区表:{}", tableName));
        df.write()
                .mode(SaveMode.Overwrite)
                .format(dataSourceRegister.shortName())
                .saveAsTable(tableName);
        log.info("表:{} 已经写完", this.tableName);
    }


    /**
     * 读单分区表的数据
     *
     * @param partition 分区
     * @return
     */
    public Dataset<Row> read(String partition) {
        return read(Lists.newArrayList(partition));
    }


    /**
     * 读单分区表的多个分区
     *
     * @param partitions
     * @return
     */
    public Dataset<Row> read(List<String> partitions) {
        checkIsSinglePartitionTable();
        checkNotEmpty(Lists.newArrayList(partitionNames), "分区名不能为空");
        checkNotEmpty(partitions, "分区列表不能为空");
        for (String partition : partitions) {
            assertPartitionExist(partition);
        }
        log.info("读分区表:{} 分区{}:{}", tableName, getPartitionName(), partitions);
        return sparkSession.table(this.tableName).where(functions.col(getPartitionName()).isin(partitions.toArray()));
    }

    /**
     * 读这张表的所有数据
     *
     * @return
     */
    public Dataset<Row> read() {
        log.info("读表:{}", tableName);
        return sparkSession.table(this.tableName);
    }

    public Boolean isTableExist() {
        try {
            sparkSession.table(tableName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    public void drop() {
        SessionCatalog catalog = getCatalog();
        TableIdentifier idt = getTableIdentifier();
        log.info("开始删除表:{}", this.tableName);
        catalog.dropTable(idt, false, true);
        log.info("删除表:{} 完成", this.tableName);
    }


    protected Column[] getTableCol() {
        return toColumns(getTableColName());
    }

    protected List<String> getTableColName() {
        StructType schema = getTableSchema();
        return Lists.newArrayList(schema.fieldNames());
    }

    protected StructType getTableSchema() {
        Dataset<Row> t = sparkSession.table(this.tableName);
        try {
            CatalogTable tableMete = t.sparkSession().sessionState().catalog().getTableMetadata(getTableIdentifier());
            return tableMete.schema();
        } catch (NoSuchTableException e) {
            throw new RuntimeException(format("表名:{}不存在", tableName), e);
        } catch (NoSuchDatabaseException e) {
            throw new RuntimeException(format("库名:{}不存在", tableName), e);
        }
    }

    protected Boolean isFieldExist(Dataset<Row> df, final String field) {
        try {
            df.schema().fieldIndex(field);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }


    private void checkIsPartitionTable() {
        if (!partitionTable) {
            throw new RuntimeException(format("该表[{}]不是分区表", tableName));
        }
    }

    private void checkIsSinglePartitionTable() {
        if (!singlePartitionTable) {
            throw new RuntimeException(format("该表[{}]不是单分区表", tableName));
        }
    }

    protected void removeByPartition(Map<String, String> partitionInfo) {
        String sqlT = "ALTER TABLE %s DROP IF EXISTS PARTITION(%s)";
        List<String> pair = new ArrayList<>();
        for (Map.Entry<String, String> entry : partitionInfo.entrySet()) {
            checkNotBlank(entry.getKey());
            checkNotBlank(entry.getValue());
            pair.add(String.format("%s = '%s'", entry.getKey(), entry.getValue()));
        }
        String sql = String.format(sqlT, tableName, StringUtils.join(pair, ","));
        sparkSession.sql(sql);
    }

    protected void removeByPartition(String partitionName, String partition) {
        removeByPartition(new HashMap<String, String>() {{
            put(partitionName, partition);
        }});
        log.info("删除表:{} 分区名:{} 分区:{}", this.tableName, partitionName, partition);
    }


    private Boolean isPartitionExist(String partition) {
        try {
            assertPartitionExist(partition);
        } catch (Throwable e) {
            return false;
        }
        return true;
    }

    /**
     * TODO:正确实现
     *
     * @param partition
     */
    private void assertPartitionExist(String partition) {
        SessionCatalog catalog = getCatalog();
        catalog.listPartitionsByFilter(getTableIdentifier(), javaListToSeq(Lists.newArrayList()));
    }


    private TableIdentifier getTableIdentifier() {
        try {
            TableIdentifier identifier = sparkSession.sessionState().sqlParser().parseTableIdentifier(this.tableName);
            return identifier;
        } catch (ParseException e) {
            String msg = format("获取表:{} 的TableIdentifier异常", this.tableName);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private CatalogTable getTableMeta() {
        try {
            CatalogTable meta = getCatalog().getTableMetadata(getTableIdentifier());
            return meta;
        } catch (NoSuchTableException | NoSuchDatabaseException e) {
            throw new RuntimeException(format("表:{} 在hive不存在", tableName), e);
        }
    }

    /**
     * 判断字段名称是否一致
     */
    private Boolean isFieldSame(Dataset<Row> dataset) {
        List<String> tableFs = getTableColName();
        List<String> datasetFs = getDatasetFieldNames(dataset);
        try {
            checkSameElement(tableFs, datasetFs);
            return true;
        } catch (Throwable e) {
            sort(tableFs);
            sort(datasetFs);
            log.warn("表[{}]的字段为:{}, 数据的字段为:{}", tableName, tableFs, datasetFs);
            return false;
        }
    }

    private List<String> getDatasetFieldNames(Dataset<Row> dataset) {
        return Lists.newArrayList(dataset.schema().fieldNames());
    }

    private List<StructField> getDatasetFields(Dataset<Row> dataset) {
        return Lists.newArrayList(dataset.schema().fields());
    }

    /**
     * 判断dataset里面的字段是否比表里面的少
     *
     * @param dataset
     * @return
     */
    private Boolean isLessField(Dataset<Row> dataset) {
        return !getLessField(dataset).isEmpty();
    }

    /**
     * 获取dataset比表少的字段
     *
     * @param dataset
     * @return
     */
    private List<String> getLessField(Dataset<Row> dataset) {
        List<String> less = new ArrayList<>();
        List<String> tableFs = getTableColName();
        List<String> datasetFs = Lists.newArrayList(dataset.schema().fieldNames());
        for (String col : tableFs) {
            if (!datasetFs.contains(col)) {
                less.add(col);
            }
        }
        return less;
    }

    /**
     * 获取新增的字段
     *
     * @param dataset
     * @return
     */
    private List<String> findNewFields(Dataset<Row> dataset) {
        List<String> newFields = new ArrayList<>();
        List<String> tableFs = getTableColName();
        List<String> datasetFs = Lists.newArrayList(dataset.schema().fieldNames());
        for (String fields : datasetFs) {
            if (!tableFs.contains(fields)) {
                newFields.add(fields);
            }
        }
        return newFields;
    }

    private void sort(List<String> collection) {
        collection.sort(Comparator.naturalOrder());
    }


    private SessionCatalog getCatalog() {
        SessionCatalog catalog = sparkSession.sessionState().catalog();
        return catalog;
    }

    private String getPartitionName() {
        return partitionNames.get(0);
    }
}