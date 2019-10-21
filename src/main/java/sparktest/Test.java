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
		final Path path = SparkExampleDataFiles.getFlightsSummary2015();

		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<String> logData = spark.read().textFile(path.toString());//.cache();

		final long numAs = logData.filter((final String s) -> s.contains("a")).count();
		final long numBs = logData.filter((final String s) -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		// Lines with a: 256, lines with b: 34

		logData.show(5);
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

		final Dataset<Row> peopleJson = spark.read().json(SparkExampleDataFiles.getPeopleJson().toString());
		peopleJson.show();
//		+----+-------+
//		| age|   name|
//		+----+-------+
//		|null|Michael|
//		|  30|   Andy|
//		|  19| Justin|
//		+----+-------+

		final Dataset<Row> peopleText = spark.read().text(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleText.show();
//		+-----------+
//		|      value|
//		+-----------+
//		|Michael, 29|
//		|   Andy, 30|
//		| Justin, 19|
//		+-----------+

		final Dataset<Row> peopleCsv = spark.read().csv(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleCsv.show();
//		+-------+---+
//		|    _c0|_c1|
//		+-------+---+
//		|Michael| 29|
//		|   Andy| 30|
//		| Justin| 19|
//		+-------+---+

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

		analyseJavaFiles(spark);

		spark.stop();
	}

	private static void analyseJavaFiles(final SparkSession spark) {
		// change me
		final Path sourceDirectory = Paths.get("C:\\eclipse\\2019-09\\ws\\spark-test\\src\\main\\java");
		if (Files.notExists(sourceDirectory)) {
			return;
		}
		final String extension = "java";
		final String[] javaFiles = getAllFilesFromPathWithExtensionAsStringArray(sourceDirectory, extension);
		final Dataset<String> dsJavaFiles = spark.read().textFile(javaFiles);
		dsJavaFiles.show(); // shows all files
		System.out.println("Lines in Java source files: " + dsJavaFiles.count());

		// final Predicate<String> keepNonEmpty = s -> !s.trim().isEmpty(); // unfortunately, this makes the task non-serialisable :/
		final FlatMapFunction<String, String> lineToWords = l -> Arrays.stream(l.split("\\s+")).filter(s -> !s.trim().isEmpty()).iterator();
		final Dataset<String> dsWords = dsJavaFiles.flatMap(lineToWords, Encoders.STRING());
		System.out.println("Words in Java source files: " + dsWords.count());

		dsWords.groupBy("value").count().sort(functions.desc("count")).show();
		// example output:
//		+--------------------+
//		|               value|
//		+--------------------+
//		|  package sparktest;|
//		|                    |
//		|import java.util....|
//		|import java.util....|
//		|import java.util....|
//		|                    |
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|import org.apache...|
//		|                    |
//		|public class Basi...|
//		+--------------------+
//		only showing top 20 rows
//
//		Lines in Java source files: 949
//		Words in Java source files: 3222
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
//		only showing top 20 rows
	}

	private static final int MAX_DEPTH = 999;

	public static List<Path> getAllFilesFromWithExtension(final Path dir, final String extension) {
		final BiPredicate<Path, BasicFileAttributes> matcher = (p, bfa) -> bfa.isRegularFile() && p.toString().endsWith(extension);
		try {
			return Files.find(dir, MAX_DEPTH, matcher).collect(Collectors.toList());
		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return Collections.emptyList();
	}

	/** Return type String[] for spark {@link DataFrameReader#textFile(String...)} */
	public static String[] getAllFilesFromPathWithExtensionAsStringArray(final Path dir, final String extension) {
		return getAllFilesFromWithExtension(dir, extension).stream().map(p -> p.toString()).toArray(String[]::new);
	}

	public static String[] getAllJavaFilesFromPathAsStringArray(final Path dir) {
		return getAllFilesFromPathWithExtensionAsStringArray(dir, "java");
	}
}