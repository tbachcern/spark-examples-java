package sparktest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.sun.xml.txw2.IllegalSignatureException;

/**
 * Convenience class to intercept System.out
 *
 * @author tbach
 */
public class ConsoleRedirector {
	private final PrintStream originalSystemOut;
	private final ByteArrayOutputStream myOutputStream;
	private final PrintStream myPrintStream;

	//	System.setOut(originalSystemOut); // restore original stream
	private ConsoleRedirector() {
		myOutputStream = new ByteArrayOutputStream();
		myPrintStream = new PrintStream(myOutputStream, true);
		originalSystemOut = System.out;
	}

	public static ConsoleRedirector newAndActivate() {
		return new ConsoleRedirector().activate();
	}

	public ConsoleRedirector activate() {
		if (originalSystemOut == null) {
			throw new IllegalSignatureException("original System.out stream is null?");
		}
		System.setOut(myPrintStream);
		return this; // allow chaining
	}

	public void resetToOriginalStream() {
		if (originalSystemOut == null) {
			throw new IllegalSignatureException("original System.out stream is null?");
		}
		System.setOut(originalSystemOut);
	}

	/**
	 * Gets everything that was printed with System.out.print as a string.<br>
	 * Then, resets the buffer so that the next call to this method does not contain the current result.<br>
	 * Also, we avoid a memory leak with this behavior.<br>
	 */
	public String getLastPrintedStringAndResetBuffer() {
		myPrintStream.flush(); // should be autoflush, but let us be sure
		final String result = myOutputStream.toString();
		myOutputStream.reset();
		return result;
	}

	/**
	 * Calls {@link #getLastPrintedStringAndResetBuffer()} to gain string.<br>
	 * Calls {@link #printlnToConsole(String)} with string<br>
	 * Returns string<br>
	 */
	public String getLastStringPrintToConsoleAndResetBuffer() {
		final String result = getLastPrintedStringAndResetBuffer();
		printlnToConsole(result);
		return result;
	}

	/** Prints to the original console */
	public void printlnToConsole(final String string) {
		originalSystemOut.println(string);
	}

	/** Prints to the original console */
	public void printlnToConsole(final Object object) {
		originalSystemOut.println(object);
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		// we try our best to restore the original if someone forgets it
		// but really, this is bad
		// we should use AutoCloaseable, but this adds one additional layer of indentation which is not wanted now
		if (System.out != originalSystemOut) {
			resetToOriginalStream();
		}
		super.finalize();
	}
}
