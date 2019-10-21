package sparktest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import sparktest.SparkProviderLocal.SparkConfig;

/**
 * Provides access to example files.
 *
 * Spark requires a path to a file as a String. However, we use (the more recent) Path here.<br>
 * One may convert via {@link Path#toString()}.<br>
 *<br>
 * Internally, this provides the example files from spark-example.<br>
 * Spark load files from a path, therefore, we read the files from the jar and copy them into {@link SparkConfig#PATH_HADOOP}.<br>
 *
 * @author tbach
 */
public class SparkExampleDataFiles {

	public static Path getKv1Txt() {
		return copyAndGetPath("kv1.txt");
	}

	public static Path getPeopleJson() {
		return copyAndGetPath("people.json");
	}

	public static Path getPeopleTxt() {
		return copyAndGetPath("people.txt");
	}

	public static Path getUserAvsc() {
		return copyAndGetPath("user.avsc");
	}

	public static Path getUsersAvro() {
		return copyAndGetPath("users.avro");
	}

	public static Path getFlightsSummary2015() {
		return SparkConfig.PATH_HADOOP.resolve("2015-summary.csv");
	}

	private static synchronized Path copyAndGetPath(final String fileName) {
		final Path path = SparkConfig.PATH_HADOOP.resolve(fileName);
		if (Files.exists(path)) {
			return path;
		}
		// try to read the example files from spark-examples
		// which is done by ClassLoader.getSystemResourceAsStream(fileName)
		// because they are in the root of the spark-examples project
		try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(fileName);
				InputStreamReader streamReader = new InputStreamReader(inputStream);
				BufferedReader bufferedReader = new BufferedReader(streamReader)) {
			final List<String> lines = bufferedReader.lines().collect(Collectors.toList());
			Files.write(path, lines);
		} catch (final IOException e) {
			System.err.println("File reading failed for: " + fileName);
			e.printStackTrace();
		}
		if (Files.notExists(path)) {
			System.err.println("Path does not exist: " + path);
		}
		return path;
	}

	public static void main(final String[] args) throws IOException {
		Files.delete(getPeopleJson());
		getPeopleJson();
	}
}
