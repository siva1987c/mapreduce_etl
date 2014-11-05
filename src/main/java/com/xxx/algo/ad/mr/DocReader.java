package com.xxx.algo.ad.mr;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * A class that provides an multi-line document reader from an input stream.
 * Document records should be separated using an entire line with explicit
 * marks.
 */
public class DocReader {
	private byte[] EndMark;
	private final byte[] LineEnd = { '\n' };

	private LineReader in;

	/**
	 * Creates a new instance of DocReader using the given
	 * <code>Configuration</code>.
	 * 
	 * @param in
	 *            The input stream
	 * @param mark
	 *            The end mark
	 * @throws IOException
	 */
	public DocReader(InputStream in, byte[] mark) throws IOException {
		this.EndMark = mark;
		this.in = new LineReader(in);
	}

	/**
	 * Read an document from the InputStream into the given Text.
	 * 
	 * @param str
	 *            the object to store the given document
	 * @return int the number of bytes read
	 * @throws IOException
	 */
	public int readDoc(Text str) throws IOException {
		str.clear();
		long bytesConsumed = 0;

		Text Line = new Text();
		do {
			int len = in.readLine(Line);
			if (len == 0) {
				break;
			}
			bytesConsumed += len;
			str.append(Line.getBytes(), 0, Line.getLength());
			str.append(LineEnd, 0, LineEnd.length);
		} while (Line.compareTo(EndMark, 0, EndMark.length) != 0);

		return (int) bytesConsumed;
	}

	/**
	 * Close the underlying stream.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
	}

}
