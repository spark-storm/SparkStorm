package me.decken.sparkstorm.app;

import lombok.extern.slf4j.Slf4j;
import me.decken.sparkstorm.boot.SimpleLocalBoot;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

/**
 * @author decken
 * @date 2019-05-23 21:51
 */
@Slf4j
public class LRDemo {

    public static void main(String[] args) {
        // Every record of this DataFrame contains the label and
        // features represented by a vector.
        SimpleLocalBoot boot = new SimpleLocalBoot("lr-demo");
        boot.init();
        StructType schema = new StructType(new StructField[]{
                new StructField("labels", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> data = boot.spark()
                .read()
                .format("libsvm")
                .load("data/sample_linear_regression_data.txt");
        data = data.withColumn("label", when(col("label").geq(0.5), 1).otherwise(0));

        data.show(false);

        // Set parameters for the algorithm.
        // Here, we limit the number of iterations to 10.
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Fit the model to the data.
        LogisticRegressionModel model = lr.fit(data);

        // Inspect the model: get the feature weights.
        Vector weights = model.coefficients();
        log.info("权重数:{} 非零权重:{}", weights.size(), weights.numNonzeros());

        // Given a dataset, predict each point's label, and show the results.
        model.transform(data).show(false);
    }
}
