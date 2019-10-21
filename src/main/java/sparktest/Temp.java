package sparktest;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

public class Temp {
	public static void main(final String[] args) {
		final OutputStream myOutputStream = new ByteArrayOutputStream();
		final PrintStream myPrintStream = new PrintStream(myOutputStream, true); // true=autoFlush
		final PrintStream originalSystemOut = System.out; // save original stream
		System.setOut(myPrintStream); // use our stream

		System.setOut(originalSystemOut); // restore original stream
	}
}
