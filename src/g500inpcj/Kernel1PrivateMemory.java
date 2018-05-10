package g500inpcj;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.pcj.PCJ;

public class Kernel1PrivateMemory implements Kernel1 {

	private final static int CHUNK_SIZE = 1 << 30; // 1 GB

	private G500InPCJ storage;

	// Local
	long[][] myGraphChunks;
	private long localMaxVertex;

	private long[] xendoff;

	private long nv;
	private int lastNumerOfBitsShift;
	private long lastNumerOfBitsMask;

	private static final String DEGREES_RECV = "k1DegreesReceived";
	private static final String DEGREES_RECV_PARTS = "howManyk1DegreesReceived";

	private static final String ADJ_RECV = "k1AdjReceived";
	private static final String ADJ_RECV_PARTS = "howManyk1AdjReceived";

	public Kernel1PrivateMemory(G500InPCJ storage) {
		this.storage = storage;
		localMaxVertex = 0;
		if (PCJ.myId() == 0) {
			storage.localMaxVertices = new long[PCJ.threadCount()];
		}
		storage.taskFromVertexArray = new int[PCJ.threadCount()];

	}

	public void compute(String edgeListFilePath) {
		int k1receivedLength = (65536 * 2) / PCJ.threadCount();
		storage.k1DegreesReceived = new int[PCJ.threadCount()][k1receivedLength][];
		storage.howManyk1DegreesReceived = new int[PCJ.threadCount()];

		storage.k1AdjReceived = new int[PCJ.threadCount()][k1receivedLength][];
		storage.howManyk1AdjReceived = new int[PCJ.threadCount()];

		// From Graph500 specification: "... the kernel is provided only the
		// edge list and the edge list's size. Further information such as
		// the number of vertices must be computed within this kernel."
		findMaxVertex();

		int log2N = 64 - Long.numberOfLeadingZeros(storage.globalMaxVertex);
		if (PCJ.myId() == 0)
			System.out.println("SCALE = " + log2N);
		nv = 1 << log2N;
		int log2ThreadCount = 32 - Integer.numberOfLeadingZeros(PCJ
				.threadCount() - 1);
		lastNumerOfBitsShift = log2N - log2ThreadCount;
		lastNumerOfBitsMask = (1L << lastNumerOfBitsShift) - 1L;

		// Compute CSR
		computeCSR();

		// KernelsHelper.printDummyGraphOutcomes(storage.xadj, storage.xoff,
		// new long[0]);
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

			// System.out.println(PCJ.myId() + ": edges on task = "
			// + myNumberOfEdgesOnTask);

			long numberOfBytesToRead = myNumberOfEdgesOnTask * 16L;
			long howManyChunks = (long) Math.ceil((double) numberOfBytesToRead
					/ CHUNK_SIZE);

			myGraphChunks = new long[(int) howManyChunks][];

			// System.out.println(PCJ.myId() + ": chunks = " + howManyChunks);

			long readFrom = computeStartOfReadingPosition(avgNumberOfEdgesOnTask);
			long readTo = readFrom + numberOfBytesToRead;
			// System.out.println(PCJ.myId() + ": start reading from byte = "
			// + readFrom + " to " + readTo);
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
				// System.out.println(PCJ.myId() + " thisSegmentSize "
				// + thisSegmentSize);
				bytesRead = data.read(buff);
				// System.out.println(PCJ.myId() + " bytesRead " + bytesRead);
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

		// System.out.println(PCJ.myId() + ": max local = " + localMaxVertex);

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

		// System.out.println(PCJ.myId() + ": max global SIMPLE = "
		// + storage.globalMaxVertex);
	}

	private void computeCSR() {

		AllTasksBufferWrapper bufferDegrees = new AllTasksBufferWrapper(
				DEGREES_RECV, DEGREES_RECV_PARTS);
		long to = findTaskToVertexArrayExclusive(PCJ.myId());
		long from = findTaskFromVertexArrayInclusive(PCJ.myId());

		// PCJ.log("from  " + from + " to " + to);

		storage.xoff = new long[(int) (to - from)];

		for (int i = 0; i < myGraphChunks.length; i++) {
			for (int j = 0; j < myGraphChunks[i].length; j = j + 2) {

				long v = myGraphChunks[i][j];
				long w = myGraphChunks[i][j + 1];
				if (v != w) {
					int ownerV = this.findTaskForGlobalVertexNumber(v);
					int ownerW = this.findTaskForGlobalVertexNumber(w);
					if (PCJ.myId() != ownerV) {
						bufferDegrees.send(ownerV, (int) v);
					} else {
						++storage.xoff[findLocalVertexNumberForGlobalVertexNumber(v)];
					}
					if (PCJ.myId() != ownerW) {
						bufferDegrees.send(ownerW, (int) w);
					} else {
						++storage.xoff[findLocalVertexNumberForGlobalVertexNumber(w)];
					}
				}
			}
		}
		bufferDegrees.sendAllRemaining();

		int degreesRecvParts = PCJ.waitFor(DEGREES_RECV_PARTS, 0);
		while (degreesRecvParts < PCJ.threadCount()) {
			computeDegreesFromK1PartsReceived(false);
			degreesRecvParts = PCJ.waitFor(DEGREES_RECV_PARTS, 0);
		}
		PCJ.monitor(DEGREES_RECV_PARTS);

		int howManyChunksWeShouldWaitFor = 0;
		for (int i = 0; i < storage.howManyk1DegreesReceived.length; i++) {
			howManyChunksWeShouldWaitFor += storage.howManyk1DegreesReceived[i];
		}

		int receivedPredCounter = PCJ.waitFor(DEGREES_RECV, 0);
		while (receivedPredCounter < howManyChunksWeShouldWaitFor) {
			computeDegreesFromK1PartsReceived(false);
			receivedPredCounter = PCJ.waitFor(DEGREES_RECV, 0);
		}
		PCJ.monitor(DEGREES_RECV);
		computeDegreesFromK1PartsReceived(true);

		// KernelsHelper.printDummyGraphOutcomes(new long[0], storage.xoff,
		// new long[0]);
		/********************************************/

		xendoff = new long[storage.xoff.length];
		xendoff[0] = 0;
		for (int k = 1; k < xendoff.length; k++) {
			xendoff[k] = storage.xoff[k - 1];
			storage.xoff[k] += storage.xoff[k - 1];
		}

		storage.xadj = new long[(int) storage.xoff[storage.xoff.length - 1]];
		long where;

		/********************************************/

		AllTasksBufferWrapper bufferAdj = new AllTasksBufferWrapper(ADJ_RECV,
				ADJ_RECV_PARTS);

		for (int i = 0; i < myGraphChunks.length; i++) {
			for (int j = 0; j < myGraphChunks[i].length; j = j + 2) {

				long v = myGraphChunks[i][j];
				long w = myGraphChunks[i][j + 1];
				if (v != w) {
					int ownerV = this.findTaskForGlobalVertexNumber(v);
					int ownerW = this.findTaskForGlobalVertexNumber(w);
					if (PCJ.myId() != ownerV) {
						bufferAdj.send(ownerV, (int) v);
						bufferAdj.send(ownerV, (int) w);
					} else {
						where = xendoff[findLocalVertexNumberForGlobalVertexNumber(v)]++;
						storage.xadj[(int) where] = w;
					}
					if (PCJ.myId() != ownerW) {
						bufferAdj.send(ownerW, (int) w);
						bufferAdj.send(ownerW, (int) v);
					} else {
						where = xendoff[findLocalVertexNumberForGlobalVertexNumber(w)]++;
						storage.xadj[(int) where] = v;
					}
				}
			}
		}
		bufferAdj.sendAllRemaining();

		int adjRecvParts = PCJ.waitFor(ADJ_RECV_PARTS, 0);
		while (adjRecvParts < PCJ.threadCount()) {
			computeAdjFromK1PartsReceived(false);
			adjRecvParts = PCJ.waitFor(ADJ_RECV_PARTS, 0);
		}
		PCJ.monitor(ADJ_RECV_PARTS);

		int howManyAdjChunksWeShouldWaitFor = 0;
		for (int i = 0; i < storage.howManyk1AdjReceived.length; i++) {
			howManyAdjChunksWeShouldWaitFor += storage.howManyk1AdjReceived[i];
		}

		int receivedAdjCounter = PCJ.waitFor(ADJ_RECV, 0);
		while (receivedAdjCounter < howManyAdjChunksWeShouldWaitFor) {
			computeAdjFromK1PartsReceived(false);
			receivedAdjCounter = PCJ.waitFor(ADJ_RECV, 0);
		}
		PCJ.monitor(ADJ_RECV);
		computeAdjFromK1PartsReceived(true);
	}

	private void computeAdjFromK1PartsReceived(boolean last) {
		long where;
		for (int i = 0; i < storage.k1AdjReceived.length; i++) {
			for (int j = 0; j < storage.k1AdjReceived[i].length; j++) {
				int[] receivedArray = storage.k1AdjReceived[i][j];
				if (receivedArray != null) {
					if (receivedArray.length == AllTasksBufferWrapper.BUFFER_LENGTH
							|| last) {
						for (int k = 0; k < receivedArray.length; k = k + 2) {
							where = xendoff[findLocalVertexNumberForGlobalVertexNumber(receivedArray[k])]++;
							storage.xadj[(int) where] = receivedArray[k + 1];
						}
						storage.k1AdjReceived[i][j] = new int[0];
					}
				} else {
					break;
				}
			}
		}
	}

	private void computeDegreesFromK1PartsReceived(boolean last) {
		for (int i = 0; i < storage.k1DegreesReceived.length; i++) {
			for (int j = 0; j < storage.k1DegreesReceived[i].length; j++) {
				int[] receivedArray = storage.k1DegreesReceived[i][j];
				if (receivedArray != null) {
					if (receivedArray.length == AllTasksBufferWrapper.BUFFER_LENGTH
							|| last) {
						for (int k = 0; k < receivedArray.length; k++) {
							++storage.xoff[findLocalVertexNumberForGlobalVertexNumber(receivedArray[k])];
						}
						storage.k1DegreesReceived[i][j] = new int[0];
					}
				} else {
					break;
				}
			}
		}
	}

	private long findTaskFromVertexArrayInclusive(long taskId) {
		return taskId << lastNumerOfBitsShift;
	}

	private long findTaskToVertexArrayExclusive(long taskId) {
		return (taskId + 1L) << lastNumerOfBitsShift;
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

	@Override
	public long getNv() {
		return nv;
	}
}
