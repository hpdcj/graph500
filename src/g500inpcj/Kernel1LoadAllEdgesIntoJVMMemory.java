package g500inpcj;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.pcj.Group;
import org.pcj.PCJ;

public class Kernel1LoadAllEdgesIntoJVMMemory implements Kernel1 {

	private final static int CHUNK_SIZE = 1 << 30; // 1 GB
	static long[][] wholeGraph;

	private G500InPCJ storage;

	// Local
	private long localMaxVertex;

	private long nv;
	private long avgNumberOfVerticesOnTask;
	private long left;
	private long irregular;
	private long to, from; // inclusive

	private Group myJVMGroup;

	public Kernel1LoadAllEdgesIntoJVMMemory(Group g, G500InPCJ storage) {
		this.myJVMGroup = g;
		this.localMaxVertex = 0;
		this.storage = storage;
		if (PCJ.myId() == 0) {
			storage.localMaxVertices = new long[PCJ.threadCount()];
		}
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

		// Compute CSR
		computeCSR();
		// KernelsHelper.printDummyGraphOutcomes(xadj, xoff, new long[0]);
	}

	public void loadAllEdgesIntoJVMMemoryByOneTaskOnJVM(String filePath) {
		if (myJVMGroup.myId() == 0) {
			File file = new File(filePath);
			// Every edge (two vertices) occupies 16 bytes (16 = 2 * 8
			// bytes),
			// as every vertex occupies 8 bytes
			// if (file.length() % 16 == 0) {
			// throw new RuntimeException("File length  != 0 mod 16 ! "
			// + file.length());
			// }
			long allNumberOfEdges = file.length() >> 4; // divide by 16
			System.out.println("Number of edges in file: " + allNumberOfEdges
					+ " numer of bytes: " + file.length());
			long avgNumberOfEdgesOnTask = allNumberOfEdges / PCJ.threadCount();
			int edgesLeft = (int) (allNumberOfEdges - PCJ.threadCount()
					* avgNumberOfEdgesOnTask);
			int howManyAllChunks = 0;
			for (int taskId = 0; taskId < PCJ.threadCount(); taskId++) {
				long numberOfEdgesOnTask;
				if (taskId == (PCJ.threadCount() - 1)) {
					numberOfEdgesOnTask = avgNumberOfEdgesOnTask + edgesLeft;
				} else {
					numberOfEdgesOnTask = avgNumberOfEdgesOnTask;
				}

				// System.out.println("edges on task " + taskId + " = "
				// + numberOfEdgesOnTask);

				long numberOfBytesToRead = numberOfEdgesOnTask * 16L;
				int howManyChunks = (int) Math
						.ceil((double) numberOfBytesToRead / CHUNK_SIZE);

				if (myJVMGroup.getGroupName().equals("Group 0")) {
					PCJ.put(taskId, "wholeGraphFrom", howManyAllChunks);
					PCJ.put(taskId, "wholeGraphTo", howManyAllChunks
							+ howManyChunks);
				}
				howManyAllChunks += howManyChunks;
			}

			try (BufferedInputStream data = new BufferedInputStream(
					new FileInputStream(filePath))) {

				wholeGraph = new long[howManyAllChunks][];

				System.out.println(PCJ.myId() + ": all chunks = "
						+ howManyAllChunks);
				int chunkNumber = 0;
				long bytesRead = 0;
				byte[] buff;
				int pos;
				int chunkNumberIdx;
				for (int taskId = 0; taskId < PCJ.threadCount(); taskId++) {
					long readFrom = computeStartOfReadingPosition(taskId,
							avgNumberOfEdgesOnTask);

					long numberOfEdgesOnTask;
					if (taskId == (PCJ.threadCount() - 1)) {
						numberOfEdgesOnTask = avgNumberOfEdgesOnTask
								+ edgesLeft;
					} else {
						numberOfEdgesOnTask = avgNumberOfEdgesOnTask;
					}

					long readTo = readFrom + numberOfEdgesOnTask * 16L;

					for (long offset = readFrom; offset < readTo; offset += CHUNK_SIZE) {
						long remainingBytesToRead = readTo - offset;
						int thisSegmentSize = (int) Math.min(CHUNK_SIZE,
								remainingBytesToRead);

						wholeGraph[chunkNumber] = new long[thisSegmentSize >> 3];
						buff = new byte[thisSegmentSize];
						bytesRead += data.read(buff);
						pos = 0;
						chunkNumberIdx = 0;
						while (pos < buff.length) {
							long v = KernelsHelper.shiftByteOrder(buff, pos);
							pos = pos + 8;
							wholeGraph[chunkNumber][chunkNumberIdx++] = v;
						}
						chunkNumber++;
					}
				}
				System.out.println("Read " + bytesRead + " in chunk parts = "
						+ chunkNumber);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		PCJ.waitFor("wholeGraphFrom");
		PCJ.waitFor("wholeGraphTo");
		System.out.println(PCJ.myId() + " : wholeGraphChunksFrom = "
				+ storage.wholeGraphFrom + " wholeGraphChunksTo = "
				+ storage.wholeGraphTo);
		PCJ.barrier();
		// wholeGraphsChunks();
	}

	private long computeStartOfReadingPosition(int taskId,
			long avgNumberOfEdgesOnTask) {
		return taskId * avgNumberOfEdgesOnTask * 16L;
	}

	// find local max, reduce and get global max
	private void findMaxVertex() {

		int pos;
		for (int i = storage.wholeGraphFrom; i < storage.wholeGraphTo; i++) {
			pos = 0;
			while (pos < wholeGraph[i].length) {

				long v = wholeGraph[i][pos++];

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
	}

	// compute XOFF from whole graph
	private void computeXOFF() {

		long to = this.to + 1;
		long from = this.from;

		storage.xoff = new long[(int) (to - from)];
		int pos;
		for (int i = 0; i < wholeGraph.length; i++) {
			pos = 0;
			while (pos < wholeGraph[i].length) {

				long v = wholeGraph[i][pos++];
				long w = wholeGraph[i][pos++];

				if (v != w) { /* Skip self-edges. */
					if (v >= from && v < to) {
						++storage.xoff[(int) (v - from)];
					}
					if (w >= from && w < to) {
						++storage.xoff[(int) (w - from)];
					}
				}
			}
		}
	}

	private void computeXADJ() {

		long to = this.to + 1;
		long from = this.from;

		long[] xendoff = new long[(int) (to - from)];
		xendoff[0] = 0;
		for (int k = 1; k < xendoff.length; k++) {
			xendoff[k] = storage.xoff[k - 1];
			storage.xoff[k] += storage.xoff[k - 1];
		}

		storage.xadj = new long[(int) storage.xoff[storage.xoff.length - 1]];
		long where;
		int pos;
		for (int i = 0; i < wholeGraph.length; i++) {
			pos = 0;
			while (pos < wholeGraph[i].length) {

				long v = wholeGraph[i][pos++];
				long w = wholeGraph[i][pos++];

				if (v != w) { /* Skip self-edges. */
					if (v >= from && v < to) {
						where = xendoff[(int) (v - from)]++;
						storage.xadj[(int) where] = w;
					}
					if (w >= from && w < to) {
						where = xendoff[(int) (w - from)]++;
						storage.xadj[(int) where] = v;
					}
				}
			}
			pos = 0;
		}
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
