package g500inpcj;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.pcj.PCJ;

class Kernel1LoadChunkOfEdgesIntoPrivateMemory implements Kernel1 {

	private final static int CHUNK_SIZE = 1 << 30; // 1 GB

	private G500InPCJ storage;

	// Local
	long[][] myGraphChunks;
	private long localMaxVertex;

	private long nv;
	private long avgNumberOfVerticesOnTask;
	private long left;
	private long irregular;
	private long to, from; // inclusive

	long[] xoffForAllLocal;
	long[] xadjForAllLocal;

	public Kernel1LoadChunkOfEdgesIntoPrivateMemory(G500InPCJ storage) {
		this.storage = storage;
		localMaxVertex = 0;
		if (PCJ.myId() == 0) {
			storage.localMaxVertices = new long[PCJ.threadCount()];
		}
		storage.xoffGatheredFromTasks = new long[PCJ.threadCount()][];
		storage.xadjGatheredFromTasks = new long[PCJ.threadCount()][];
		storage.taskFromVertexArray = new int[PCJ.threadCount()];
	}

	public void compute(String edgeListFilePath) {
		// From Graph500 specification: "... the kernel is provided only the
		// edge list and the edge list's size. Further information such as
		// the number of vertices must be computed within this kernel."
		findMaxVertex();

		nv = storage.globalMaxVertex + 1;
		avgNumberOfVerticesOnTask = nv / PCJ.threadCount();
		long myNumberOfVerticesOnTask = avgNumberOfVerticesOnTask;
		left = nv - PCJ.threadCount() * myNumberOfVerticesOnTask;
		irregular = left * (avgNumberOfVerticesOnTask + 1);
		if (left > PCJ.myId()) {
			myNumberOfVerticesOnTask++;
			from = PCJ.myId() * myNumberOfVerticesOnTask;
		} else {
			from = PCJ.myId() * myNumberOfVerticesOnTask + left;
		}
		to = from + myNumberOfVerticesOnTask - 1;

		for (int i = 0; i < PCJ.threadCount(); i++)
			PCJ.put(i, "taskFromVertexArray", (int) from, PCJ.myId());

		xoffForAllLocal = new long[(int) nv];

		// Compute CSR
		computeCSR();
	}

	public void loadChunkOfEdgesIntoPrivateMemory(String filePath) {

		File file = new File(filePath);

		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {
			// Every edge (two vertices) occupies 16 bytes (16 = 2 * 8 bytes),
			// as every vertex occupies 8 bytes
			if (file.length() % 16 != 0) {
				throw new RuntimeException("File length  != 0 mod 16 ! "
						+ file.length());
			}
			long allNumberOfEdges = file.length() >> 4; // divide by 16
			if (PCJ.myId() == 0) {
				System.out.println("Number of edges in file:"
						+ allNumberOfEdges);
			}
			long avgNumberOfEdgesOnTask = allNumberOfEdges / PCJ.threadCount();
			int edgesLeft = (int) (allNumberOfEdges - PCJ.threadCount()
					* avgNumberOfEdgesOnTask);
			long myNumberOfEdgesOnTask;
			if (PCJ.myId() == (PCJ.threadCount() - 1)) {
				myNumberOfEdgesOnTask = avgNumberOfEdgesOnTask + edgesLeft;
			} else {
				myNumberOfEdgesOnTask = avgNumberOfEdgesOnTask;
			}

			System.out.println(PCJ.myId() + ": edges on task = "
					+ myNumberOfEdgesOnTask);

			long numberOfBytesToRead = myNumberOfEdgesOnTask * 16L;
			long howManyChunks = (long) Math.ceil((double) numberOfBytesToRead
					/ CHUNK_SIZE);

			myGraphChunks = new long[(int) howManyChunks][];

			System.out.println(PCJ.myId() + ": chunks = " + howManyChunks);

			long readFrom = computeStartOfReadingPosition(avgNumberOfEdgesOnTask);
			long readTo = readFrom + numberOfBytesToRead;
			System.out.println(PCJ.myId() + ": start reading from byte = "
					+ readFrom + " to " + readTo);
			int chunkNumber = 0;
			data.skip(readFrom);
			byte[] buff;
			int pos;
			int chunkNumberIdx;
			long bytesRead;
			for (long offset = readFrom; offset < readTo; offset += CHUNK_SIZE) {
				long remainingBytesToRead = readTo - offset;
				int thisSegmentSize = (int) Math.min(CHUNK_SIZE,
						remainingBytesToRead);
				myGraphChunks[chunkNumber] = new long[thisSegmentSize >> 3];
				buff = new byte[thisSegmentSize];
				System.out.println(PCJ.myId() + " thisSegmentSize "
						+ thisSegmentSize);
				bytesRead = data.read(buff);
				System.out.println(PCJ.myId() + " bytesRead " + bytesRead);
				pos = 0;
				chunkNumberIdx = 0;
				while (pos < buff.length) {
					long v = KernelsHelper.shiftByteOrder(buff, pos);
					pos = pos + 8;
					myGraphChunks[chunkNumber][chunkNumberIdx++] = v;
				}
				chunkNumber++;
			}
			if (chunkNumber != howManyChunks)
				throw new RuntimeException("Number of chunks is not correct!");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private long computeStartOfReadingPosition(long avgNumberOfEdgesOnTask) {
		return PCJ.myId() * avgNumberOfEdgesOnTask * 16L;
	}

	private void findMaxVertex() {

		for (int i = 0; i < myGraphChunks.length; i++) {
			for (int j = 0; j < myGraphChunks[i].length; j++) {

				long v = myGraphChunks[i][j];

				if (v > localMaxVertex) {
					localMaxVertex = v;
				}
			}
		}

		System.out.println(PCJ.myId() + ": max local = " + localMaxVertex);

		PCJ.put(0, "localMaxVertices", localMaxVertex, PCJ.myId());

		if (PCJ.myId() == 0) {
			PCJ.waitFor("localMaxVertices", PCJ.threadCount());
			storage.globalMaxVertex = storage.localMaxVertices[0];
			for (int i = 1; i < storage.localMaxVertices.length; i++) {
				if (storage.localMaxVertices[i] > storage.globalMaxVertex) {
					storage.globalMaxVertex = storage.localMaxVertices[i];
				}
			}
			PCJ.broadcast("globalMaxVertex", storage.globalMaxVertex);
		}
		PCJ.waitFor("globalMaxVertex");

		System.out.println(PCJ.myId() + ": max global SIMPLE = "
				+ storage.globalMaxVertex);
	}

	private void computeCSR() {
		computeXOFF();
		computeXADJ();

		// Compute xoff
		PCJ.waitFor("xoffGatheredFromTasks", PCJ.threadCount());
		storage.xoff = new long[(int) (to + 1 - from)];
		for (int i = 0; i < storage.xoffGatheredFromTasks.length; i++) {
			for (int j = 0; j < storage.xoffGatheredFromTasks[i].length; j++) {
				storage.xoff[j] += storage.xoffGatheredFromTasks[i][j];
			}
		}
		long[] xendoff = new long[storage.xoff.length];
		xendoff[0] = 0;
		for (int k = 1; k < xendoff.length; k++) {
			xendoff[k] = storage.xoff[k - 1];
			storage.xoff[k] += storage.xoff[k - 1];
		}

		// Compute xadj
		PCJ.waitFor("xadjGatheredFromTasks", PCJ.threadCount());
		storage.xadj = new long[(int) storage.xoff[storage.xoff.length - 1]];
		int where;
		for (int i = 0; i < storage.xoffGatheredFromTasks.length; i++) {
			int index = 0;
			for (int j = 0; j < storage.xoffGatheredFromTasks[i].length; j++) {
				for (int k = 0; k < storage.xoffGatheredFromTasks[i][j]; k++) {
					where = (int) xendoff[j]++;
					storage.xadj[where] = storage.xadjGatheredFromTasks[i][index++];
				}
			}
		}

		// KernelsHelper.printDummyGraphOutcomes(xadj, xoff, xendoff);

	}

	private void computeXOFF() {
		for (int i = 0; i < myGraphChunks.length; i++) {
			for (int j = 0; j < myGraphChunks[i].length; j = j + 2) {

				long v = myGraphChunks[i][j];
				long w = myGraphChunks[i][j + 1];
				if (v != w) {
					// (int) from - the code is not verified for graphs SCALE >
					// 30
					xoffForAllLocal[(int) v]++;
					xoffForAllLocal[(int) w]++;
				}
			}
		}

		for (int taskId = 0; taskId < PCJ.threadCount(); taskId++) {
			long[] arrayToSend = Arrays
					.copyOfRange(
							xoffForAllLocal,
							KernelsHelper
									.safeLongToInt(storage.taskFromVertexArray[taskId]),
							KernelsHelper
									.safeLongToInt(computeTaskToVertexArrayExclusive(taskId)));
			PCJ.put(taskId, "xoffGatheredFromTasks", arrayToSend, PCJ.myId());
		}

	}

	private void computeXADJ() {
		long[] xendoffLocal = new long[xoffForAllLocal.length];
		xendoffLocal[0] = 0;
		for (int k = 1; k < xendoffLocal.length; k++) {
			xendoffLocal[k] = xoffForAllLocal[k - 1];
			xoffForAllLocal[k] += xoffForAllLocal[k - 1];
		}

		long xadjForAllLocalLength = xoffForAllLocal[xoffForAllLocal.length - 1];
		System.out.println(PCJ.myId() + " xadjForAllLocalLength = "
				+ xadjForAllLocalLength);
		xadjForAllLocal = new long[KernelsHelper
				.safeLongToInt(xadjForAllLocalLength)];
		System.out.println(PCJ.myId() + " alloced xadjForAllLocal");
		long where;
		for (int i = 0; i < myGraphChunks.length; i++) {
			for (int j = 0; j < myGraphChunks[i].length; j = j + 2) {

				long v = myGraphChunks[i][j];
				long w = myGraphChunks[i][j + 1];
				if (v != w) { /* Skip self-edges. */
					where = xendoffLocal[(int) v]++;
					xadjForAllLocal[(int) where] = (int) w;
					where = xendoffLocal[(int) w]++;
					xadjForAllLocal[(int) where] = (int) v;
				}
			}
		}

		// Send
		long fromIndexForTaskInXadjForAllLocal = 0;
		long toIndexForTaskInXadjForAllLocal;
		System.out.println(PCJ.myId() + " sending iwith put in k1");
		for (int taskId = 0; taskId < PCJ.threadCount(); taskId++) {
			toIndexForTaskInXadjForAllLocal = fromIndexForTaskInXadjForAllLocal
					+ xendoffLocal[KernelsHelper
							.safeLongToInt(computeTaskToVertexArrayExclusive(taskId) - 1)];
			if (taskId > 0) {
				toIndexForTaskInXadjForAllLocal -= xendoffLocal[computeTaskToVertexArrayExclusive(taskId - 1) - 1];
			}
			long[] arrayToSend = Arrays.copyOfRange(xadjForAllLocal,
					KernelsHelper
							.safeLongToInt(fromIndexForTaskInXadjForAllLocal),
					KernelsHelper
							.safeLongToInt(toIndexForTaskInXadjForAllLocal));
			// System.out.println(PCJ.myId() + "--> Sending xadj Part to "
			// + taskId + " tab = " + Arrays.toString(arrayToSend)
			// + " from " + fromIndexForTaskInXadjForAllLocal + " to "
			// + toIndexForTaskInXadjForAllLocal);
			PCJ.put(taskId, "xadjGatheredFromTasks", arrayToSend, PCJ.myId());
			fromIndexForTaskInXadjForAllLocal = toIndexForTaskInXadjForAllLocal;
		}
	}

	private int computeTaskToVertexArrayExclusive(int taskId) {
		return (taskId == PCJ.threadCount() - 1) ? xoffForAllLocal.length
				: storage.taskFromVertexArray[taskId + 1];
	}

	@Override
	public long getNv() {
		return nv;
	}

	@Override
	public int findLocalVertexNumberForGlobalVertexNumber(
			long globalVertexNumber) {
		throw new RuntimeException("Not prepared for further changes in K2");
	}

	@Override
	public int findTaskForGlobalVertexNumber(long globalVertexNumber) {
		throw new RuntimeException("Not prepared for further changes in K2");
	}

	@Override
	public long findGlobalVertexNumberForLocalVertexNumber(long taskId,
			long localVertexNumber) {
		throw new RuntimeException("Not prepared for further changes in K2");
	}

}
