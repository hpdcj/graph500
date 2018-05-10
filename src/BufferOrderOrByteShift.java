import g500inpcj.KernelsHelper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

public class BufferOrderOrByteShift {

	private static byte[][] wholeGraphChunks;
	private final static int CHUNK_SIZE = 1 << 30; // 1 GB

	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		readEdgesIntoMemory(args[0]);

		for (int i = 0; i < 10; i++) {
			long startTime = System.currentTimeMillis();
			int max = findMaxWithByteShift();
			long endTime = System.currentTimeMillis();
			System.out.println("Byte shift - max = " + max + " "
					+ (endTime - startTime) / 1000 + "s");

			startTime = System.currentTimeMillis();
			max = findMaxWithBufferOrderLongBuffer();
			endTime = System.currentTimeMillis();
			System.out.println("Buffer Order Long Buffer - max = " + max + " "
					+ (endTime - startTime) / 1000 + "s");

			startTime = System.currentTimeMillis();
			max = findMaxWithBufferOrderGetLong();
			endTime = System.currentTimeMillis();
			System.out.println("Buffer Order Get Long - max = " + max + " "
					+ (endTime - startTime) / 1000 + "s");

			System.out.println();
		}
	}

	private static int findMaxWithBufferOrderLongBuffer() {
		int maxVertex = 0;
		for (int i = 0; i < wholeGraphChunks.length; i++) {
			LongBuffer lb = ByteBuffer.wrap(wholeGraphChunks[i])
					.order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
			while (lb.hasRemaining()) {

				long v = lb.get();

				if (v > maxVertex) {
					maxVertex = (int) v;
				}
			}
		}
		return maxVertex;
	}

	private static int findMaxWithBufferOrderGetLong() {
		int maxVertex = 0;
		for (int i = 0; i < wholeGraphChunks.length; i++) {
			ByteBuffer bb = ByteBuffer.wrap(wholeGraphChunks[i]).order(
					ByteOrder.LITTLE_ENDIAN);
			while (bb.hasRemaining()) {

				long v = bb.getLong();

				if (v > maxVertex) {
					maxVertex = (int) v;
				}
			}
		}
		return maxVertex;
	}

	private static int findMaxWithByteShift() {
		int pos = 0;
		int maxVertex = 0;
		for (int i = 0; i < wholeGraphChunks.length; i++) {
			while (pos < wholeGraphChunks[i].length) {

				long v = KernelsHelper.shiftByteOrder(wholeGraphChunks, i, pos);
				pos = pos + 8;

				if (v > maxVertex) {
					maxVertex = (int) v;
				}
			}
			pos = 0;
		}
		return maxVertex;
	}

	private static void readEdgesIntoMemory(String filePath)
			throws FileNotFoundException, IOException {
		File file = new File(filePath);
		// Every edge (two vertices) occupies 16 bytes (16 = 2 * 8
		// bytes),
		// as every vertex occupies 8 bytes
		if (!isDivisibleBy16(file.length())) {
			throw new RuntimeException("File length  != 0 mod 16 ! "
					+ file.length());
		}

		long numberOfBytesToRead = file.length();
		int howManyChunks = (int) Math.ceil((double) numberOfBytesToRead
				/ CHUNK_SIZE);
		int chunkNumber = 0;
		long bytesRead = 0;
		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {

			wholeGraphChunks = new byte[howManyChunks][];
			for (long offset = 0; offset < numberOfBytesToRead; offset += CHUNK_SIZE) {
				long remainingBytesToRead = numberOfBytesToRead - offset;
				int thisSegmentSize = (int) Math.min(CHUNK_SIZE,
						remainingBytesToRead);
				wholeGraphChunks[chunkNumber] = new byte[thisSegmentSize];
				bytesRead += data.read(wholeGraphChunks[chunkNumber]);
				chunkNumber++;
			}
		}
		System.out.println("Read " + bytesRead + " in chunk parts = "
				+ chunkNumber);
	}

	private static boolean isPowerOf2(long n) {
		if ((n > 0) && ((n & (n - 1)) == 0)) {
			// number is power of 2
			return true;
		}
		return false;
	}

	private static boolean isDivisibleBy16(long n) {
		for (int i = 0; i < 4; i++) {
			if (!isPowerOf2(n >> i)) {
				return false;
			}
		}
		return true;
	}
}
