package sparktest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Simplifies the access to a local spark session and context
 * @author tbach
 */
public enum SparkProviderLocal {
	INSTANCE; // Bloch-style singleton pattern

	public static class SparkConfig {
		/** supposed to be hadoop root. However, we only need the winutils in /bin/ - must be adapted */
		public static final Path PATH_HADOOP = Paths.get("C:\\eclipse\\2019-09\\ws\\spark-test\\src\\main\\resources");
	}

	private final SparkSession sparkSession = createSparkSession();
	private final JavaSparkContext sparkContext = createOrGetSparkContext();

	/** returns a unique, local {@link SparkSession} */
	public SparkSession getSparkSession() {
		return sparkSession;
	}

	/** returns a unique {@link JavaSparkContext} for a local session */
	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}

	private static SparkSession createSparkSession() {
		if (Files.notExists(SparkConfig.PATH_HADOOP)) {
			throw new IllegalStateException("hadoop path does not exist: " + SparkConfig.PATH_HADOOP);
		}
		System.setProperty("hadoop.home.dir", SparkConfig.PATH_HADOOP.toString());
		final SparkSession spark = SparkSession.builder().appName("Simple Application")//
				.config("spark.master", "local")//
//				.config("spark.logConf", "true")// show current config
				.getOrCreate();
		spark.sparkContext().setLogLevel("WARN");
		return spark;
	}

	private JavaSparkContext createOrGetSparkContext() {
		return new JavaSparkContext(sparkSession.sparkContext());
	}

	public void stop() {
		sparkSession.stop();
	}
}
