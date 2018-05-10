package g500inpcj;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.pcj.PCJ;

public class Kernel1ReadFile implements Kernel1 {
	private final static int CHUNK_SIZE = 1 << 30; // 1 GB

	private G500InPCJ storage;

	private long nv;
	private int lastNumerOfBitsShift;
	private long lastNumerOfBitsMask;

	public Kernel1ReadFile(G500InPCJ storage) {
		this.storage = storage;
	}

	public void compute(String filePath) {
		long globalMaxVertex = findMaxVertex(filePath);

		int log2N = 64 - Long.numberOfLeadingZeros(globalMaxVertex);
		if (PCJ.myId() == 0)
			System.out.println("SCALE = " + log2N);
		nv = 1 << log2N;
		int log2ThreadCount = 32 - Integer.numberOfLeadingZeros(PCJ
				.threadCount() - 1);
		lastNumerOfBitsShift = log2N - log2ThreadCount;
		lastNumerOfBitsMask = (1L << lastNumerOfBitsShift) - 1L;

		// Compute CSR
		computeCSR(filePath);
		// KernelsHelper.printDummyGraphOutcomes(storage.xadj, storage.xoff,
		// new long[0]);

	}

	private long findMaxVertex(String filePath) {
		long maximumVertex = -1;
		// int bufferLength = (int) Math.min(CHUNK_SIZE,
		// new File(filePath).length());
		int bufferLength = 512000;
		byte[] buffer = new byte[bufferLength];
		int pos = 0;

		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {
			while (true) {
				int numberOfBytesRead = data.read(buffer);
				if (numberOfBytesRead < 0) {
					// EOF
					break;
				}
				while (pos < numberOfBytesRead) {

					long v = KernelsHelper.shiftByteOrder(buffer, pos);
					if (v > maximumVertex) {
						maximumVertex = v;
					}
					pos = pos + 8;
				}
				pos = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return maximumVertex;
	}

	private void computeCSR(String filePath) {

		long to = findTaskToVertexArrayExclusive(PCJ.myId());
		long from = findTaskFromVertexArrayInclusive(PCJ.myId());

		storage.xoff = new long[(int) (to - from)];

		int bufferLength = (int) Math.min(CHUNK_SIZE,
				new File(filePath).length());
		bufferLength = 512000;
		byte[] buffer = new byte[bufferLength];
		int pos = 0;
		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {
			while (true) {
				int numberOfBytesRead = data.read(buffer);
				if (numberOfBytesRead < 0) {
					// EOF
					break;
				}
				while (pos < numberOfBytesRead) {

					long i = KernelsHelper.shiftByteOrder(buffer, pos);
					pos = pos + 8;
					long j = KernelsHelper.shiftByteOrder(buffer, pos);
					pos = pos + 8;

					if (i != j) { /* Skip self-edges. */
						if (i >= from && i < to) {
							++storage.xoff[(int) (i - from)];
						}
						if (j >= from && j < to) {
							++storage.xoff[(int) (j - from)];
						}
					}
				}
				pos = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		// KernelsHelper.printDummyGraphOutcomes(new long[0], storage.xoff,
		// new long[0]);

		/*******************************************/

		long[] xendoff = new long[storage.xoff.length];
		xendoff[0] = 0;
		for (int k = 1; k < xendoff.length; k++) {
			xendoff[k] = storage.xoff[k - 1];
			storage.xoff[k] += storage.xoff[k - 1];
		}

		storage.xadj = new long[(int) storage.xoff[storage.xoff.length - 1]];
		long where;
		/*******************************************/

		pos = 0;
		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {
			while (true) {
				int numberOfBytesRead = data.read(buffer);
				if (numberOfBytesRead < 0) {
					// EOF
					break;
				}
				while (pos < numberOfBytesRead) {

					long i = KernelsHelper.shiftByteOrder(buffer, pos);
					pos = pos + 8;
					long j = KernelsHelper.shiftByteOrder(buffer, pos);
					pos = pos + 8;

					if (i != j) { /* Skip self-edges. */
						if (i >= from && i < to) {
							where = xendoff[(int) (i - from)]++;
							storage.xadj[(int) where] = j;
						}
						if (j >= from && j < to) {
							where = xendoff[(int) (j - from)]++;
							storage.xadj[(int) where] = i;
						}
					}
				}
				pos = 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private long findTaskFromVertexArrayInclusive(long taskId) {
		return taskId << lastNumerOfBitsShift;
	}

	private long findTaskToVertexArrayExclusive(long taskId) {
		return (taskId + 1L) << lastNumerOfBitsShift;
	}

	public long getNv() {
		return nv;
	}

	public long getLastNumerOfBitsMask() {
		return lastNumerOfBitsMask;
	}

	public long getLastNumerOfBitsShift() {
		return lastNumerOfBitsShift;
	}

	@Override
	public int findLocalVertexNumberForGlobalVertexNumber(
			long globalVertexNumber) {
		return (int) (globalVertexNumber & lastNumerOfBitsMask);
	}

	@Override
	public int findTaskForGlobalVertexNumber(long globalVertexNumber) {
		return (int) (globalVertexNumber >> lastNumerOfBitsShift);
	}

	@Override
	public long findGlobalVertexNumberForLocalVertexNumber(long taskId,
			long localVertexNumber) {
		return (taskId << lastNumerOfBitsShift) | localVertexNumber;
	}

}
