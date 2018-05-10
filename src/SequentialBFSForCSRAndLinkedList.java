import g500inpcj.KernelsHelper;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

public class SequentialBFSForCSRAndLinkedList {

	static class GraphLinkedList {
		private LinkedList<Integer>[] adj; // Adjacency Lists
		private int[] pred;
		private boolean[] visited;

		GraphLinkedList() {
			int N = findMax() + 1;
			createADJ(N);
			pred = new int[N];
			Arrays.fill(pred, -2);
			visited = new boolean[N];
		}

		private void createADJ(int N) {
			adj = new LinkedList[N];
			for (int i = 0; i < N; i++) {
				adj[i] = new LinkedList<Integer>();
			}
			for (int i = 0; i < wholeGraph.length; i++) {
				for (int j = 0; j < wholeGraph[i].length; j = j + 2) {

					long v = wholeGraph[i][j];
					long w = wholeGraph[i][j + 1];

					adj[(int) v].add((int) w);
					adj[(int) w].add((int) v);
				}
			}
		}

		void BFS(int s) {
			int numberOfVisitedVertices = 1;
			LinkedList<Integer> queue = new LinkedList<>();
			visited[s] = true;
			pred[s] = -1;
			queue.add(s);

			while (queue.size() != 0) {
				s = queue.poll();
				Iterator<Integer> i = adj[s].listIterator();
				while (i.hasNext()) {
					int n = i.next();
					if (!visited[n]) {
						visited[n] = true;
						numberOfVisitedVertices++;
						pred[n] = s;
						queue.add(n);
					}
				}
			}

			System.out.println("Number of visited vertices  = "
					+ numberOfVisitedVertices);
		}

	}

	static class GraphCSR {
		private int[] xoff;
		private int[] xadj;
		private int[] pred;
		private boolean[] visited;

		GraphCSR() {
			int N = findMax() + 1;
			createCSR(N);
			pred = new int[N];
			Arrays.fill(pred, -2);
			visited = new boolean[N];
		}

		private void createCSR(int N) {
			xoff = new int[N];
			for (int i = 0; i < wholeGraph.length; i++) {
				for (int j = 0; j < wholeGraph[i].length; j = j + 2) {

					long v = wholeGraph[i][j];
					long w = wholeGraph[i][j + 1];
					if (v != w) { /* Skip self-edges. */
						++xoff[(int) v];
						++xoff[(int) w];
					}
				}
			}

			int[] xendoff = new int[N];
			xendoff[0] = 0;
			for (int k = 1; k < xendoff.length; k++) {
				xendoff[k] = xoff[k - 1];
				xoff[k] += xoff[k - 1];
			}

			xadj = new int[xoff[xoff.length - 1]];
			int where;
			for (int i = 0; i < wholeGraph.length; i++) {
				for (int j = 0; j < wholeGraph[i].length; j = j + 2) {

					long v = wholeGraph[i][j];
					long w = wholeGraph[i][j + 1];
					if (v != w) { /* Skip self-edges. */
						where = xendoff[(int) v]++;
						xadj[where] = (int) w;
						where = xendoff[(int) w]++;
						xadj[where] = (int) v;
					}
				}
			}
		}

		void BFS(int s) {
			int numberOfVisitedVertices = 1;
			LinkedList<Integer> queue = new LinkedList<>();
			visited[s] = true;
			pred[s] = -1;
			queue.add(s);

			while (queue.size() != 0) {
				s = queue.poll();
				for (int i = getADJIndexFrom(s); i < xoff[s]; i++) {
					int n = xadj[i];
					if (!visited[n]) {
						visited[n] = true;
						numberOfVisitedVertices++;
						pred[n] = s;
						queue.add(n);
					}
				}
			}

			System.out.println("Number of visited vertices  = "
					+ numberOfVisitedVertices);
		}

		private int getADJIndexFrom(int vertexNumber) {
			if (vertexNumber > 0) {
				return xoff[vertexNumber - 1];
			} else {
				return 0;
			}
		}
	}

	private static long[][] wholeGraph;
	private final static int CHUNK_SIZE = 1 << 30; // 1 GB

	public static void main(String[] args) throws IOException {

		readEdgesIntoMemory(args[0]);
		int[] graphRoots = KernelsHelper.readToArrayBFSGraphRoots(args[0]
				+ "_root", 10);
		System.out.println();

		for (int i = 0; i < 10; i++) {
			int s = graphRoots[i];
			System.out.println("Starting test for source s = " + s);

			long startTime = System.currentTimeMillis();
			GraphLinkedList graphLinkedList = new GraphLinkedList();
			long endTime = System.currentTimeMillis();
			System.out.println("Linked List - create " + (endTime - startTime)
					/ 1000 + "s");

			startTime = System.currentTimeMillis();
			graphLinkedList.BFS(s);
			endTime = System.currentTimeMillis();
			System.out.println("Linked List - BFS " + (endTime - startTime)
					/ 1000 + "s");

			startTime = System.currentTimeMillis();
			GraphCSR graphCSR = new GraphCSR();
			endTime = System.currentTimeMillis();
			System.out.println("CSR - create " + (endTime - startTime) / 1000
					+ "s");

			startTime = System.currentTimeMillis();
			graphCSR.BFS(s);
			endTime = System.currentTimeMillis();
			System.out.println("CSR - BFS " + (endTime - startTime) / 1000
					+ "s");

			System.out.println();
		}

	}

	private static int findMax() {
		int maxVertex = 0;
		for (int i = 0; i < wholeGraph.length; i++) {
			for (int j = 0; j < wholeGraph[i].length; j++) {

				long v = wholeGraph[i][j];

				if (v > maxVertex) {
					maxVertex = (int) v;
				}
			}
		}
		return maxVertex;
	}

	private static void readEdgesIntoMemory(String filePath)
			throws FileNotFoundException, IOException {
		File file = new File(filePath);
		// Every edge (two vertices) occupies 16 bytes (16 = 2 * 8
		// bytes), as every vertex occupies 8 bytes
		if (!isDivisibleBy16(file.length())) {
			throw new RuntimeException("File length  != 0 mod 16 ! "
					+ file.length());
		}

		long numberOfBytesToRead = file.length();
		int howManyChunks = (int) Math.ceil((double) numberOfBytesToRead
				/ CHUNK_SIZE);
		int chunkNumber = 0;
		long bytesRead = 0;
		int chunkNumberIdx;
		int pos;
		try (BufferedInputStream data = new BufferedInputStream(
				new FileInputStream(filePath))) {

			wholeGraph = new long[howManyChunks][];
			byte[] buff;
			for (long offset = 0; offset < numberOfBytesToRead; offset += CHUNK_SIZE) {
				long remainingBytesToRead = numberOfBytesToRead - offset;
				int thisSegmentSizeInBytes = (int) Math.min(CHUNK_SIZE,
						remainingBytesToRead);

				wholeGraph[chunkNumber] = new long[thisSegmentSizeInBytes >> 3];
				buff = new byte[thisSegmentSizeInBytes];
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
