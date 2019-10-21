package sparktest;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Shows pipeline, estimators and transformators. Follows the spark documentation.
 * @author tbach
 */
public class LecturePipeline {
	public static void main(final String[] args) {
		estimatorTransformer();
//        pipeline();
	}

	public static void estimatorTransformer() {
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();

		// Prepare training data.
		final List<Row> dataTraining = List.of(//
				RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)), //
				RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)), //
				RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)), //
				RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))//
		);
		final StructType schema = new StructType(new StructField[] { //
				new StructField("label", DataTypes.DoubleType, false, Metadata.empty()), //
				new StructField("features", new VectorUDT(), false, Metadata.empty()) //
		});
		final Dataset<Row> trainingData = spark.createDataFrame(dataTraining, schema);

		// Create a LogisticRegression instance. This instance is an Estimator.
		final LogisticRegression logisticRegression = new LogisticRegression();
		// Print out the parameters, documentation, and any default values.
		System.out.println("LogisticRegression parameters:\n" + logisticRegression.explainParams() + "\n");

		// We may set parameters using setter methods.
		logisticRegression.setMaxIter(10).setRegParam(0.01);

		// Learn a LogisticRegression model. This uses the parameters stored in lr.
		final LogisticRegressionModel model1 = logisticRegression.fit(trainingData);
		// Since model1 is a Model (i.e., a Transformer produced by an Estimator),
		// we can view the parameters it used during fit().
		// This prints the parameter (name: value) pairs, where names are unique IDs for this
		// LogisticRegression instance.
		System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

		// We may alternatively specify parameters using a ParamMap.
		final ParamMap paramMap = new ParamMap()//
				.put(logisticRegression.maxIter().w(20)) // Specify 1 Param.
				.put(logisticRegression.maxIter(), 30) // This overwrites the original maxIter.
				.put(logisticRegression.regParam().w(0.1), logisticRegression.threshold().w(0.55)); // Specify multiple Params.

		// One can also combine ParamMaps.
		final ParamMap paramMap2 = new ParamMap().put(logisticRegression.probabilityCol().w("myProbability")); // Change output column name
		final ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

		// Now learn a new model using the paramMapCombined parameters.
		// paramMapCombined overrides all parameters set earlier via lr.set* methods.
		final LogisticRegressionModel model2 = logisticRegression.fit(trainingData, paramMapCombined);
		System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

		// Prepare test documents.
		final List<Row> dataTest = List.of(//
				RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)), //
				RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)), //
				RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))//
		);
		final Dataset<Row> test = spark.createDataFrame(dataTest, schema);

		// Make predictions on test documents using the Transformer.transform() method.
		// LogisticRegression.transform will only use the 'features' column.
		// Note that model2.transform() outputs a 'myProbability' column instead of the usual
		// 'probability' column since we renamed the lr.probabilityCol parameter previously.
		final Dataset<Row> results = model2.transform(test);
		final Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
//        results.show();
		rows.show();
//        +--------------+-----+--------------------+----------+
//        |      features|label|       myProbability|prediction|
//        +--------------+-----+--------------------+----------+
//        |[-1.0,1.5,1.3]|  1.0|[0.05707304171034...|       1.0|
//        |[3.0,2.0,-0.1]|  0.0|[0.92385223117041...|       0.0|
//        |[0.0,2.2,-1.5]|  1.0|[0.10972776114779...|       1.0|
//        +--------------+-----+--------------------+----------+
		for (final Row r : rows.collectAsList()) {
			System.out.println(String.format("(%s, %s) -> prob=%s, prediction=%s", r.get(0), r.get(1), r.get(2), r.get(3)));
		}
//        ([-1.0,1.5,1.3], 1.0) -> prob=[0.057073041710340625,0.9429269582896593], prediction=1.0
//        ([3.0,2.0,-0.1], 0.0) -> prob=[0.9238522311704118,0.07614776882958811], prediction=0.0
//        ([0.0,2.2,-1.5], 1.0) -> prob=[0.10972776114779748,0.8902722388522026], prediction=1.0
	}

	public static void pipeline() {
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();

		// Prepare training documents, which are labeled.
		final Dataset<Row> training = spark.createDataFrame(List.of(//
				new JavaLabeledDocument(0L, "a b c d e spark", 1.0), //
				new JavaLabeledDocument(1L, "b d", 0.0), new JavaLabeledDocument(2L, "spark f g h", 1.0), //
				new JavaLabeledDocument(3L, "hadoop mapreduce", 0.0)//
		), JavaLabeledDocument.class);

		// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
		final Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
		final HashingTF hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
		final LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001);
		final Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { tokenizer, hashingTF, lr });

		// Fit the pipeline to training documents.
		final PipelineModel model = pipeline.fit(training);

		// Prepare test documents, which are unlabeled.
		final Dataset<Row> test = spark.createDataFrame(List.of(//
				new JavaDocument(4L, "spark i j k"), //
				new JavaDocument(5L, "l m n"), new JavaDocument(6L, "spark hadoop spark"), //
				new JavaDocument(7L, "apache hadoop")//
		), JavaDocument.class);

		// Make predictions on test documents.
		final Dataset<Row> predictions = model.transform(test);
		final Dataset<Row> result = predictions.select("id", "text", "probability", "prediction");
		predictions.show();
		result.show();
//        +---+------------------+--------------------+--------------------+--------------------+--------------------+----------+
//        | id|              text|               words|            features|       rawPrediction|         probability|prediction|
//        +---+------------------+--------------------+--------------------+--------------------+--------------------+----------+
//        |  4|       spark i j k|    [spark, i, j, k]|(1000,[105,149,32...|[-1.6609033227473...|[0.15964077387874...|       1.0|
//        |  5|             l m n|           [l, m, n]|(1000,[6,638,655]...|[1.64218895265635...|[0.83783256854766...|       0.0|
//        |  6|spark hadoop spark|[spark, hadoop, s...|(1000,[105,181],[...|[-2.5980142174392...|[0.06926633132976...|       1.0|
//        |  7|     apache hadoop|    [apache, hadoop]|(1000,[181,495],[...|[4.00817033336806...|[0.98215753334442...|       0.0|
//        +---+------------------+--------------------+--------------------+--------------------+--------------------+----------+
//
//        +---+------------------+--------------------+----------+
//        | id|              text|         probability|prediction|
//        +---+------------------+--------------------+----------+
//        |  4|       spark i j k|[0.15964077387874...|       1.0|
//        |  5|             l m n|[0.83783256854766...|       0.0|
//        |  6|spark hadoop spark|[0.06926633132976...|       1.0|
//        |  7|     apache hadoop|[0.98215753334442...|       0.0|
//        +---+------------------+--------------------+----------+

		for (final Row r : result.collectAsList()) {
			System.out.println(String.format("(%s, %s) -> prob=%s, prediction=%s", r.get(0), r.get(1), r.get(2), r.get(3)));
		}
//        (4, spark i j k) -> prob=[0.15964077387874118,0.8403592261212589], prediction=1.0
//        (5, l m n) -> prob=[0.8378325685476612,0.16216743145233875], prediction=0.0
//        (6, spark hadoop spark) -> prob=[0.06926633132976273,0.9307336686702373], prediction=1.0
//        (7, apache hadoop) -> prob=[0.9821575333444208,0.01784246665557917], prediction=0.0
	}

	/**
	 * Labeled instance type, Spark SQL can infer schema from Java Beans.
	 */
	@SuppressWarnings("serial")
	public static class JavaLabeledDocument extends JavaDocument implements Serializable {
		private final double label;

		public JavaLabeledDocument(final long id, final String text, final double label) {
			super(id, text);
			this.label = label;
		}

		public double getLabel() {
			return label;
		}
	}

	/**
	 * Unlabeled instance type, Spark SQL can infer schema from Java Beans.
	 */
	@SuppressWarnings("serial")
	public static class JavaDocument implements Serializable {
		private final long id;
		private final String text;

		public JavaDocument(final long id, final String text) {
			this.id = id;
			this.text = text;
		}

		public long getId() {
			return id;
		}

		public String getText() {
			return text;
		}
	}
}
