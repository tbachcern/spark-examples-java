package sparktest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Shows spark basics
 * @author tbach
 */
public class LectureBasics {
	public static void main(final String[] args) {
		// comment/uncomment to only see specific example
		flightsSummary();
		flightsTop5Destinations();
		flightsExplain();
		manualSchema();

		peopleViaRddTextSchema();
		System.exit(0);
		peopleCsv();
		peopleJson();
		peopleCsvSchema();
		peopleOperations();
		peopleSql();

		stationsUserDefinedFunctions();

		SparkProviderLocal.INSTANCE.stop();
	}

	public static void flightsSummary() {
		entry("flights summary");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final String path = SparkExampleDataFiles.getFlightsSummary2015().toString();
		final Dataset<Row> ds = spark.read()//
				.option("inferSchema", true)//
				.option("header", true)//
				.csv(path);
		ds.printSchema();
		ds.show(5);
//        root
//        |-- DEST_COUNTRY_NAME: string (nullable = true)
//        |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
//        |-- count: integer (nullable = true)
//
//       +-----------------+-------------------+-----+
//       |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
//       +-----------------+-------------------+-----+
//       |    United States|            Romania|   15|
//       |    United States|            Croatia|    1|
//       |    United States|            Ireland|  344|
//       |            Egypt|      United States|   15|
//       |    United States|              India|   62|
//       +-----------------+-------------------+-----+
		// -> result is integer and columns have names

		System.out.println("--- inferschema");
		final Dataset<Row> ds2 = spark.read()//
				.option("inferSchema", true)//
				.csv(path);
		ds2.printSchema();
		ds2.show(5);
//        root
//        |-- _c0: string (nullable = true)
//        |-- _c1: string (nullable = true)
//        |-- _c2: string (nullable = true)
//
//       +-----------------+-------------------+-----+
//       |              _c0|                _c1|  _c2|
//       +-----------------+-------------------+-----+
//       |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
//       |    United States|            Romania|   15|
//       |    United States|            Croatia|    1|
//       |    United States|            Ireland|  344|
//       |            Egypt|      United States|   15|
//       +-----------------+-------------------+-----+
		// -> result is string, not integer
		//    -- this happens because the first row is considered as a value and "count" is not an integer
		// -> not column names

		final Dataset<Row> ds3 = spark.read()//
				.option("header", true)//
				.csv(path);
		ds3.printSchema();
		ds3.show(5);
//        root
//        |-- DEST_COUNTRY_NAME: string (nullable = true)
//        |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
//        |-- count: string (nullable = true)
//
//       +-----------------+-------------------+-----+
//       |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
//       +-----------------+-------------------+-----+
//       |    United States|            Romania|   15|
//       |    United States|            Croatia|    1|
//       |    United States|            Ireland|  344|
//       |            Egypt|      United States|   15|
//       |    United States|              India|   62|
//       +-----------------+-------------------+-----+
		// count is string -- columns have names

		System.out.println("---");
		final Dataset<Row> ds3Integer = ds3.withColumn("count", ds3.col("count").cast(DataTypes.IntegerType));
		ds3Integer.printSchema();
		ds3Integer.show(5);
//        root
//        |-- DEST_COUNTRY_NAME: string (nullable = true)
//        |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
//        |-- count: integer (nullable = true)
//
//       +-----------------+-------------------+-----+
//       |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
//       +-----------------+-------------------+-----+
//       |    United States|            Romania|   15|
//       |    United States|            Croatia|    1|
//       |    United States|            Ireland|  344|
//       |            Egypt|      United States|   15|
//       |    United States|              India|   62|
//       +-----------------+-------------------+-----+
		// we cast count to integer, therefore it is integer -- columns have names

		final Dataset<Row> ds4 = spark.read()//
				.csv(path);
		ds4.printSchema();
		ds4.show(5);
//        root
//        |-- _c0: string (nullable = true)
//        |-- _c1: string (nullable = true)
//        |-- _c2: string (nullable = true)
//
//       +-----------------+-------------------+-----+
//       |              _c0|                _c1|  _c2|
//       +-----------------+-------------------+-----+
//       |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
//       |    United States|            Romania|   15|
//       |    United States|            Croatia|    1|
//       |    United States|            Ireland|  344|
//       |            Egypt|      United States|   15|
//       +-----------------+-------------------+-----+
		// all columns are of type string and all columns have no descriptive names
	}

	public static void flightsTop5Destinations() {
		entry("flights top 5 destinations");
		final ConsoleRedirector consoleRedirector = ConsoleRedirector.newAndActivate();
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final String path = SparkExampleDataFiles.getFlightsSummary2015().toString();

		// shows that the same result can be achieved via different ways using spark functions or pure sql

		final Dataset<Row> flightData2015 = spark.read()//
				.option("inferSchema", true)//
				.option("header", true)//
				.csv(path);

		// we may use the "sql" functions of spark:
		flightData2015.groupBy("DEST_COUNTRY_NAME")//
				.sum("count")//
				.withColumnRenamed("sum(count)", "dest_total")//
				.sort(functions.desc("dest_total"))//
				.limit(6)// 6 is only done to get the same result for result comparisons. 5 is the correct value
				.show(5); // show adds a "only showing top 5 rows" line to the output if #rows > 5
		final String showSqlFunctions = consoleRedirector.getLastStringPrintToConsoleAndResetBuffer();
//        +-----------------+----------+
//        |DEST_COUNTRY_NAME|dest_total|
//        +-----------------+----------+
//        |    United States|    411352|
//        |           Canada|      8399|
//        |           Mexico|      7140|
//        |   United Kingdom|      2025|
//        |            Japan|      1548|
//        +-----------------+----------+

		// we may also use the more traditional orderBy:
		spark.read().option("inferSchema", true).option("header", true).csv(path)//
				.groupBy("DEST_COUNTRY_NAME")//
				.sum("count")//
				.withColumnRenamed("sum(count)", "dest_total")//
				.orderBy(functions.desc("dest_total"))//
				.show(5);
		final String showSqlFunctions2 = consoleRedirector.getLastStringPrintToConsoleAndResetBuffer();
		consoleRedirector.printlnToConsole("1. and 2. show outputs are identical? " + showSqlFunctions.equals(showSqlFunctions2));

//        +-----------------+----------+
//        |DEST_COUNTRY_NAME|dest_total|
//        +-----------------+----------+
//        |    United States|    411352|
//        |           Canada|      8399|
//        |           Mexico|      7140|
//        |   United Kingdom|      2025|
//        |            Japan|      1548|
//        +-----------------+----------+
//		1. and 2. show outputs are identical? true

		// or we may use plain sql
		// for this, we first have to register the table with a specific name
		final String tableName = "flights2015";
		spark.read().option("inferSchema", true).option("header", true).csv(path)//
				.createOrReplaceTempView(tableName); // make it a table available for sql queries

		final String sql = String.format(//
				/*    */"SELECT DEST_COUNTRY_NAME, sum(count) as dest_total " + //
						"FROM %s " + //
						"GROUP BY DEST_COUNTRY_NAME " + //
						"ORDER BY dest_total DESC",
				tableName);
		spark.sql(sql).show(5);
		final String showPureSql = consoleRedirector.getLastStringPrintToConsoleAndResetBuffer();
		consoleRedirector.printlnToConsole("function and pure show outputs are identical? " + showSqlFunctions.equals(showPureSql));

//        +-----------------+----------+
//        |DEST_COUNTRY_NAME|dest_total|
//        +-----------------+----------+
//        |    United States|    411352|
//        |           Canada|      8399|
//        |           Mexico|      7140|
//        |   United Kingdom|      2025|
//        |            Japan|      1548|
//        +-----------------+----------+
//		function and pure show outputs are identical? true
		consoleRedirector.resetToOriginalStream();
	}

	public static void flightsExplain() {
		entry("flights explain");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final String path = SparkExampleDataFiles.getFlightsSummary2015().toString();

		spark.read().option("inferSchema", true).option("header", true).csv(path).groupBy("DEST_COUNTRY_NAME")//
				.sum("count")//
				.withColumnRenamed("sum(count)", "dest_total")//
				.sort(functions.desc("dest_total"))//
				.limit(5)//
				.explain();
//        == Physical Plan ==
//                TakeOrderedAndProject(limit=5, orderBy=[dest_total#213L DESC NULLS LAST], output=[DEST_COUNTRY_NAME#200,dest_total#213L])
//                +- *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#200], functions=[sum(cast(count#202 as bigint))])
//                   +- Exchange hashpartitioning(DEST_COUNTRY_NAME#200, 200)
//                      +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#200], functions=[partial_sum(cast(count#202 as bigint))])
//                         +- *(1) FileScan csv [DEST_COUNTRY_NAME#200,count#202] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/temp/mmd/hadoop/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,count:int>
		spark.read().option("inferSchema", true).option("header", true).csv(path)//
				.groupBy("DEST_COUNTRY_NAME")//
				.sum("count")//
				.withColumnRenamed("sum(count)", "dest_total")//
				.sort(functions.desc("dest_total"))//
				.limit(5)//
				.explain(true); // true = extended
//        == Parsed Logical Plan ==
//                GlobalLimit 5
//                +- LocalLimit 5
//                   +- Sort [dest_total#242L DESC NULLS LAST], true
//                      +- Project [DEST_COUNTRY_NAME#229, sum(count)#239L AS dest_total#242L]
//                         +- Aggregate [DEST_COUNTRY_NAME#229], [DEST_COUNTRY_NAME#229, sum(cast(count#231 as bigint)) AS sum(count)#239L]
//                            +- Relation[DEST_COUNTRY_NAME#229,ORIGIN_COUNTRY_NAME#230,count#231] csv
//
//                == Analyzed Logical Plan ==
//                DEST_COUNTRY_NAME: string, dest_total: bigint
//                GlobalLimit 5
//                +- LocalLimit 5
//                   +- Sort [dest_total#242L DESC NULLS LAST], true
//                      +- Project [DEST_COUNTRY_NAME#229, sum(count)#239L AS dest_total#242L]
//                         +- Aggregate [DEST_COUNTRY_NAME#229], [DEST_COUNTRY_NAME#229, sum(cast(count#231 as bigint)) AS sum(count)#239L]
//                            +- Relation[DEST_COUNTRY_NAME#229,ORIGIN_COUNTRY_NAME#230,count#231] csv
//
//                == Optimized Logical Plan ==
//                GlobalLimit 5
//                +- LocalLimit 5
//                   +- Sort [dest_total#242L DESC NULLS LAST], true
//                      +- Aggregate [DEST_COUNTRY_NAME#229], [DEST_COUNTRY_NAME#229, sum(cast(count#231 as bigint)) AS dest_total#242L]
//                         +- Project [DEST_COUNTRY_NAME#229, count#231]
//                            +- Relation[DEST_COUNTRY_NAME#229,ORIGIN_COUNTRY_NAME#230,count#231] csv
//
//                == Physical Plan ==
//                TakeOrderedAndProject(limit=5, orderBy=[dest_total#242L DESC NULLS LAST], output=[DEST_COUNTRY_NAME#229,dest_total#242L])
//                +- *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#229], functions=[sum(cast(count#231 as bigint))], output=[DEST_COUNTRY_NAME#229, dest_total#242L])
//                   +- Exchange hashpartitioning(DEST_COUNTRY_NAME#229, 200)
//                      +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#229], functions=[partial_sum(cast(count#231 as bigint))], output=[DEST_COUNTRY_NAME#229, sum#247L])
//                         +- *(1) FileScan csv [DEST_COUNTRY_NAME#229,count#231] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/C:/temp/mmd/hadoop/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,count:int>
	}

	public static void manualSchema() {
		entry("manual schema");

		final StructType myManualSchema = DataTypes.createStructType(new StructField[] { //
				getStringTypeWithName("some"), //
				getStringTypeWithName("col"), //
				getLongTypeWithName("names"), //
		});
		final Row myRow = RowFactory.create("Hello", null, 1L);
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> myDf = spark.createDataFrame(List.of(myRow), myManualSchema);
		myDf.show();
//        +-----+----+-----+
//        | some| col|names|
//        +-----+----+-----+
//        |Hello|null|    1|
//        +-----+----+-----+
	}

	public static void peopleViaRddTextSchema() {
		entry("people text schema via rdd");
		final JavaSparkContext sparkContext = SparkProviderLocal.INSTANCE.getSparkContext();
		final String path = SparkExampleDataFiles.getPeopleTxt().toString();
		final JavaRDD<Row> parts = sparkContext.textFile(path).map(s -> RowFactory.create((Object[]) s.split(", ")));
		final StructType peopleCsvSchema = getCsvPeopleSchema();

		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> dfPeople = spark.createDataFrame(parts, peopleCsvSchema);
		dfPeople.show();
//        +-------+---+
//        |   name|age|
//        +-------+---+
//        |Michael| 29|
//        |   Andy| 30|
//        | Justin| 19|
//        +-------+---+
	}

	public static StructType getCsvPeopleSchema() {
		final boolean canBeNull = true;
		final StructType peopleCsvSchema = DataTypes.createStructType(new StructField[] {
				// DataTypes.createStructField("name", DataTypes.StringType, true), // the boolean is "nullable"
				DataTypes.createStructField("name", DataTypes.StringType, canBeNull),
				DataTypes.createStructField("age", DataTypes.StringType, canBeNull),
				// cannot be IntegerType because "reasons"
		});
		return peopleCsvSchema;
	}

	public static void peopleCsv() {
		System.out.println("people csv");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> peopleCsv = spark.read().csv(SparkExampleDataFiles.getPeopleTxt().toString());
		peopleCsv.show();
//        +-------+---+
//        |    _c0|_c1|
//        +-------+---+
//        |Michael| 29|
//        |   Andy| 30|
//        | Justin| 19|
//        +-------+---+
	}

	public static void peopleJson() {
		System.out.println("people json");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> peopleJson = spark.read().json(SparkExampleDataFiles.getPeopleJson().toString());
		peopleJson.show();
//        +----+-------+
//        | age|   name|
//        +----+-------+
//        |null|Michael|
//        |  30|   Andy|
//        |  19| Justin|
//        +----+-------+
	}

	public static void peopleCsvSchema() {
		System.out.println("people csv with schema");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final StructType peopleCsvSchema = getCsvPeopleSchema();
		peopleCsvSchema.printTreeString();
//        root
//        |-- name: string (nullable = true)
//        |-- age: string (nullable = true)
		final String path = SparkExampleDataFiles.getPeopleTxt().toString();
		final Dataset<Row> peopleCsvWithSchema = spark.read().schema(peopleCsvSchema).csv(path);
		peopleCsvWithSchema.show();
//        +-------+---+
//        |   name|age|
//        +-------+---+
//        |Michael| 29|
//        |   Andy| 30|
//        | Justin| 19|
//        +-------+---+
	}

	public static void peopleOperations() {
		System.out.println("people json operations");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> dfPeople = spark.read().json(SparkExampleDataFiles.getPeopleJson().toString());
		dfPeople.printSchema();
//        root
//        |-- age: long (nullable = true)
//        |-- name: string (nullable = true)
		dfPeople.select("name").show();
//        +-------+
//        |   name|
//        +-------+
//        |Michael|
//        |   Andy|
//        | Justin|
//        +-------+
		dfPeople.groupBy("age").count().show();
//        +----+-----+
//        | age|count|
//        +----+-----+
//        |  19|    1|
//        |null|    1|
//        |  30|    1|
//        +----+-----+
	}

	public static void peopleSql() {
		entry("people json sql");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final Dataset<Row> dfPeople = spark.read().json(SparkExampleDataFiles.getPeopleJson().toString());
		dfPeople.filter(dfPeople.col("age").gt(25)).show();
//        +---+----+
//        |age|name|
//        +---+----+
//        | 30|Andy|
//        +---+----+

		dfPeople.createOrReplaceTempView("people");
		spark.sql("SELECT * FROM people WHERE age > 25").show();
//        +---+----+
//        |age|name|
//        +---+----+
//        | 30|Andy|
//        +---+----+

		dfPeople.sqlContext().sql("SELECT * FROM people WHERE age > 25").show();
//        +---+----+
//        |age|name|
//        +---+----+
//        | 30|Andy|
//        +---+----+
	}

	public static void stationsUserDefinedFunctions() {
		entry("stations");
		final SparkSession spark = SparkProviderLocal.INSTANCE.getSparkSession();
		final List<String> stations = new ArrayList<>();
		stations.add("2,San Jose Diridon Caltrain Station,37.329732,-121.901782,27,San Jose,8/6/2013");
		stations.add("3,San Jose Civic Center,37.330698,-121.888979,15,San Jose,8/5/2013");
		stations.add("4,Santa Clara at Almaden,37.333988,-121.894902,11,San Jose,8/6/2013");
		stations.add("5,Adobe on Almaden,-37.331415,121.8932,19,San Jose,8/5/2013");
		stations.add("6,San Pedro Square,37.336721,121.894074,15,San Jose,8/7/2013");
		final List<Row> rows = stations.stream().map(s -> RowFactory.create((Object[]) s.split(","))).collect(Collectors.toList());
		Dataset<Row> dsStations = spark.createDataFrame(rows, getSchemaStations());
		dsStations = changeColumnTypeToIntForDataSetColumn(dsStations, "id");
		dsStations = changeColumnTypeToIntForDataSetColumn(dsStations, "number");
		dsStations = changeColumnTypeForDataSetColumnToType(dsStations, "lat", DataTypes.DoubleType);
		dsStations = changeColumnTypeForDataSetColumnToType(dsStations, "lon", DataTypes.DoubleType);
		dsStations.printSchema();
//		root
//		 |-- id: integer (nullable = true)
//		 |-- name: string (nullable = true)
//		 |-- lat: double (nullable = true)
//		 |-- lon: double (nullable = true)
//		 |-- number: integer (nullable = true)
//		 |-- location: string (nullable = true)
//		 |-- date: string (nullable = true)
		dsStations.show();
//		+---+--------------------+----------+-----------+------+--------+--------+
//		| id|                name|       lat|        lon|number|location|    date|
//		+---+--------------------+----------+-----------+------+--------+--------+
//		|  2|San Jose Diridon ...| 37.329732|-121.901782|    27|San Jose|8/6/2013|
//		|  3|San Jose Civic Ce...| 37.330698|-121.888979|    15|San Jose|8/5/2013|
//		|  4|Santa Clara at Al...| 37.333988|-121.894902|    11|San Jose|8/6/2013|
//		|  5|    Adobe on Almaden|-37.331415|   121.8932|    19|San Jose|8/5/2013|
//		|  6|    San Pedro Square| 37.336721| 121.894074|    15|San Jose|8/7/2013|
//		+---+--------------------+----------+-----------+------+--------+--------+

		final UserDefinedFunction latToDir = functions.udf((final Double x) -> (x > 0 ? "N" : "S"), DataTypes.StringType);
		final UserDefinedFunction lonToDir = functions.udf((final Double x) -> (x > 0 ? "E" : "W"), DataTypes.StringType);
		dsStations.select(//
				dsStations.col("lat"), latToDir.apply(functions.col("lat")).alias("latdir"), //
				dsStations.col("lon"), lonToDir.apply(functions.col("lon")).alias("londir"))//
				.show();
//		+----------+------+-----------+------+
//		|       lat|latdir|        lon|londir|
//		+----------+------+-----------+------+
//		| 37.329732|     N|-121.901782|     W|
//		| 37.330698|     N|-121.888979|     W|
//		| 37.333988|     N|-121.894902|     W|
//		|-37.331415|     S|   121.8932|     E|
//		| 37.336721|     N| 121.894074|     E|
//		+----------+------+-----------+------+
	}

	public static StructType getSchemaStations() {
		final StructType peopleCsvSchema = DataTypes.createStructType(new StructField[] { //
				getStringTypeWithName("id"), //
				getStringTypeWithName("name"), //
				getStringTypeWithName("lat"), //
				getStringTypeWithName("lon"), //
				getStringTypeWithName("number"), //
				getStringTypeWithName("location"), //
				getStringTypeWithName("date"), //
		});
		return peopleCsvSchema;
	}

	private static final boolean CAN_BE_NULL = true;
	private static final boolean CANNOT_BE_NULL = false;

	/** Convenience method to save some typing and long lines */
	public static StructField getStringTypeWithName(final String name) {
		return DataTypes.createStructField(name, DataTypes.StringType, CAN_BE_NULL);
	}

	/** Convenience method to save some typing and long lines */
	public static StructField getLongTypeWithName(final String name) {
		return DataTypes.createStructField(name, DataTypes.LongType, CANNOT_BE_NULL);
	}

	/** Convenience method to save some typing and long lines */
	public static Dataset<Row> changeColumnTypeForDataSetColumnToType(final Dataset<Row> dataset, final String columnName,
			final DataType dataType) {
		return dataset.withColumn(columnName, dataset.col(columnName).cast(dataType));
	}

	/** Convenience method to save some typing and long lines */
	public static Dataset<Row> changeColumnTypeToIntForDataSetColumn(final Dataset<Row> dataset, final String columnName) {
		return changeColumnTypeForDataSetColumnToType(dataset, columnName, DataTypes.IntegerType);
	}

	private static void entry(final String text) {
		System.out.println("##########> " + text);
	}
}
