package sparktest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Tests the basic functionality and availability of example files.
 * @author tbach
 */
public class Test {
	public static void main(final String[] args) {
		startingExample();
		howtoReadDataFromFiles();
		findTopWordsInAllJavaFiles();

		SparkProviderLocal.INSTANCE.stop(); // shutdown spark, might be also done by shutdown of JVM
	}

	/** A simple example that shows the very basic functionality */
	public static void startingExample() {
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();

		final Path path = SparkExampleDataFiles.getFlightsSummary2015();
		final Dataset<String> dsFlights = spark.read().textFile(path.toString());

		final long numAs = dsFlights.filter((final String s) -> s.contains("a")).count();
		final long numBs = dsFlights.filter((final String s) -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		// Lines with a: 256, lines with b: 34

		dsFlights.show(5);
//		+--------------------+
//		|               value|
//		+--------------------+
//		|DEST_COUNTRY_NAME...|
//		|United States,Rom...|
//		|United States,Cro...|
//		|United States,Ire...|
//		|Egypt,United Stat...|
//		|United States,Ind...|
//		+--------------------+
	}

	/** Shows different ways to read data. {@link LectureBasics} shows more nuances */
	public static void howtoReadDataFromFiles() {
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();

		/*
		 * We read DataSets (DS), which are equivalent to DataFrames in Java
		 * A DS consists of named columns of data with associated types for each column
		 * We can show the columns with .show()
		 * We can show the types with .printSchema()
		 */

		final Dataset<Row> peopleJson = spark.read().json(SparkExampleDataFiles.getPeopleJson().toString());
		peopleJson.printSchema();
//		root
//		 |-- age: long (nullable = true)
//		 |-- name: string (nullable = true)
		peopleJson.show();
//		+----+-------+
//		| age|   name|
//		+----+-------+
//		|null|Michael|
//		|  30|   Andy|
//		|  19| Justin|
//		+----+-------+

		final Dataset<Row> peopleCsv = spark.read().csv(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleCsv.printSchema();
//		root
//		 |-- _c0: string (nullable = true)
//		 |-- _c1: string (nullable = true)
		peopleCsv.show();
//		+-------+---+
//		|    _c0|_c1|
//		+-------+---+
//		|Michael| 29|
//		|   Andy| 30|
//		| Justin| 19|
//		+-------+---+

		final Dataset<Row> peopleText = spark.read().text(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleText.printSchema();
//		root
//		 |-- value: string (nullable = true)
		peopleText.show();
//		+-----------+
//		|      value|
//		+-----------+
//		|Michael, 29|
//		|   Andy, 30|
//		| Justin, 19|
//		+-----------+

		// provide a schema for csv
		final boolean canBeNull = true;
		final StructType peopleCsvSchema = DataTypes.createStructType(new StructField[] {
				// DataTypes.createStructField("name", DataTypes.StringType, true), // the boolean is "nullable"
				DataTypes.createStructField("name", DataTypes.StringType, canBeNull),
				DataTypes.createStructField("age", DataTypes.StringType, canBeNull),
				// cannot be IntegerType because "reasons"
		});
		peopleCsvSchema.printTreeString();
//		root
//		 |-- name: string (nullable = true)
//		 |-- age: string (nullable = true)
		final Dataset<Row> peopleCsvWithSchema = spark.read().schema(peopleCsvSchema).csv(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleCsvWithSchema.show();
//		+-------+---+
//		|   name|age|
//		+-------+---+
//		|Michael| 29|
//		|   Andy| 30|
//		| Justin| 19|
//		+-------+---+
	}

	/**
	 * A more advanced example.<br>
	 * Read all Java files of this project and show the most frequently used words within these files.
	 */
	public static void findTopWordsInAllJavaFiles() {
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Path sourceDirectory = Paths.get("C:\\eclipse\\2019-09\\ws\\spark-test\\src\\main\\java"); // change me
		if (Files.notExists(sourceDirectory)) {
			return;
		}
		final String[] javaFiles = getAllJavaFilesFromPathAsStringArray(sourceDirectory);
		final Dataset<String> dsJavaFiles = spark.read().textFile(javaFiles); // spark can read multiple files
		dsJavaFiles.show(5); // shows 5 of all lines in all files
//		+--------------------+
//		|               value|
//		+--------------------+
//		|  package sparktest;|
//		|                    |
//		|import java.util....|
//		|import java.util....|
//		|import java.util....|
//		+--------------------+
		System.out.println("Lines in Java source files: " + dsJavaFiles.count());
		// Lines in Java source files: 1073 // number may be different

		// final Predicate<String> keepNonEmpty = s -> !s.trim().isEmpty(); // unfortunately, this makes the task non-serialisable :/
		final FlatMapFunction<String, String> lineToWords = l -> Arrays.stream(l.split("\\s+")).filter(s -> !s.trim().isEmpty()).iterator();
		final Dataset<String> dsWords = dsJavaFiles.flatMap(lineToWords, Encoders.STRING());
		System.out.println("Words in Java source files: " + dsWords.count());
		// Words in Java source files: 3785 // number may be different

		dsWords.groupBy("value").count().sort(functions.desc("count")).show();
//		+------------+-----+
//		|       value|count|
//		+------------+-----+
//		|          //|  325|
//		|           =|  130|
//		|       final|  109|
//		|           ||   76|
//		|           {|   73|
//		|      import|   68|
//		|           }|   67|
//		|      public|   56|
//		|      static|   43|
//		|Dataset<Row>|   32|
//		|      United|   29|
//		|      return|   27|
//		|      String|   27|
//		|     States||   26|
//		|         new|   24|
//		|          +-|   23|
//		|        void|   22|
//		|          ->|   21|
//		|       spark|   20|
//		|       true)|   19|
//		+------------+-----+
	}

	public static List<Path> getAllFilesFromPathWithExtension(final Path dir, final String extension) {
		final int maxRecursionDepth = 99;
		final BiPredicate<Path, BasicFileAttributes> matcher = //
				(path, fileAttribute) -> fileAttribute.isRegularFile() && path.toString().endsWith(extension);
		try {
			return Files.find(dir, maxRecursionDepth, matcher).collect(Collectors.toList());
		} catch (final IOException e) {
			System.err.println("Could not read from: " + dir);
			e.printStackTrace();
			System.exit(0); // give up
		}
		return Collections.emptyList();
	}

	/** Return type String[] for spark {@link DataFrameReader#textFile(String...)} */
	public static String[] getAllFilesFromPathWithExtensionAsStringArray(final Path dir, final String extension) {
		return getAllFilesFromPathWithExtension(dir, extension).stream().map(Path::toString).toArray(String[]::new);
	}

	public static String[] getAllJavaFilesFromPathAsStringArray(final Path dir) {
		return getAllFilesFromPathWithExtensionAsStringArray(dir, "java");
	}
}