package g500inpcj;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.BitSet;

import org.pcj.Group;
import org.pcj.PCJ;
import org.pcj.Shared;
import org.pcj.StartPoint;
import org.pcj.Storage;

public class G500InPCJ extends Storage implements StartPoint {

	// Kernel 1 - all
	@Shared
	long[] xoff;
	@Shared
	long[] xadj;

	// Kernel 1 - load all
	// Kernel 1 - private mem
	@Shared
	long[] localMaxVertices; // used by task 0 for reduction

	@Shared
	long globalMaxVertex;

	// Kernel 1 - load all
	@Shared
	int wholeGraphFrom; // inclusive - index of array holeGraphChunks divided by
						// number of all tasks
	@Shared
	int wholeGraphTo; // exclusive

	// Kernel 1 - private mem
	@Shared
	long[][] xoffGatheredFromTasks;
	@Shared
	long[][] xadjGatheredFromTasks;
	@Shared
	int[] taskFromVertexArray;

	// Kernel 1 - private mem NEW
	@Shared
	int[][][] k1DegreesReceived;
	@Shared
	int[] howManyk1DegreesReceived;

	@Shared
	int[][][] k1AdjReceived;
	@Shared
	int[] howManyk1AdjReceived;

	// Kernel 2
	@Shared
	BitSet[] bitmap; // bit set if vertex v is visited, otherwise 0
	@Shared
	int[][][] receivedPred;
	@Shared
	int[] howManyVerticesInNextFrontier;
	@Shared
	boolean shouldComputeNextLevel;
	@Shared
	int[] howManyPartsSent;
	@Shared
	long[] numberOfVisitedVertices;

	public static void main(String[] args) throws IOException {
		PCJ.start(G500InPCJ.class, G500InPCJ.class, args[0]);
	}

	@Override
	public void main() throws Throwable {
		runPrivateMemory();
		// runLoadAllEdgesIntoJVMMemory();
		// runLoadChunkOfEdgesIntoPrivateMemory();
		// runReadFile();
	}

	private void runPrivateMemory() {
		Kernel1PrivateMemory k1 = new Kernel1PrivateMemory(this);
		Kernel2 k2 = new Kernel2(this, k1);

		PCJ.barrier();

		final Path graphPath = Paths.get(System.getProperty("graph.path"));
		if (PCJ.myId() == 0)
			System.out.println(PCJ.myId()
					+ ": Running Graph500 for graph from file "
					+ graphPath.toString());

		// Read edges to memory
		k1.loadChunkOfEdgesIntoPrivateMemory(graphPath.toString());

		// Run kernel 1
		for (int i = 0; i < 6; i++) {
			long startTime = System.currentTimeMillis();
			k1.compute(graphPath.toString());
			long endTime = System.currentTimeMillis();

			if (PCJ.myId() == 0) {
				System.out
						.println(PCJ.myId()
								+ " of "
								+ PCJ.threadCount()
								+ " tasks -- Kernel 1 PCJ 4 done --- PRivate mem-- test "
								+ 0 + " : " + (endTime - startTime) / 1000
								+ "s");
			}
		}
		PCJ.barrier();
		k1.myGraphChunks = null;
		System.gc();
		PCJ.barrier();
		// }
		if (PCJ.myId() == 0)
			System.out.println("Kernel 1 PCJ 4  test finished");

		// Kernel 2
		int[] graphRoots = KernelsHelper.readToArrayBFSGraphRoots(
				graphPath.toString() + "_root", 5);
		k2.compute(graphRoots);

	}

	private void runLoadAllEdgesIntoJVMMemory() {
		Group g = PCJ.join("Group " + PCJ.getPhysicalNodeId());
		Kernel1LoadAllEdgesIntoJVMMemory k1 = new Kernel1LoadAllEdgesIntoJVMMemory(
				g, this);
		Kernel2 k2 = new Kernel2(this, k1);

		PCJ.barrier();

		final Path graphPath = Paths.get(System.getProperty("graph.path"));
		if (PCJ.myId() == 0)
			System.out.println(PCJ.myId()
					+ ": Running Graph500 for graph from file "
					+ graphPath.toString());

		// Read edges to memory
		k1.loadAllEdgesIntoJVMMemoryByOneTaskOnJVM(graphPath.toString());

		// Run kernel 1
		// for (int i = 0; i < 10; i++) {
		long startTime = System.currentTimeMillis();
		k1.compute(graphPath.toString());
		long endTime = System.currentTimeMillis();

		PCJ.barrier();
		Kernel1LoadAllEdgesIntoJVMMemory.wholeGraph = null;
		System.gc();
		if (PCJ.myId() == 0) {
			System.out.println(PCJ.myId() + " of " + PCJ.threadCount()
					+ " tasks -- Kernel 1 PCJ 4 done --- Load all -- test " + 0
					+ " : " + (endTime - startTime) / 1000 + "s");
		}
		PCJ.barrier();
		// }
		if (PCJ.myId() == 0)
			System.out.println("Kernel 1 PCJ 4  test finished");

		// Kernel 2
		int[] graphRoots = KernelsHelper.readToArrayBFSGraphRoots(
				graphPath.toString() + "_root", 5);
		k2.compute(graphRoots);

	}

	private void runLoadChunkOfEdgesIntoPrivateMemory() {
		Kernel1LoadChunkOfEdgesIntoPrivateMemory k1 = new Kernel1LoadChunkOfEdgesIntoPrivateMemory(
				this);
		Kernel2 k2 = new Kernel2(this, k1);

		PCJ.barrier();

		final Path graphPath = Paths.get(System.getProperty("graph.path"));
		if (PCJ.myId() == 0)
			System.out.println(PCJ.myId()
					+ ": Running Graph500 for graph from file "
					+ graphPath.toString());

		// Read edges to memory
		k1.loadChunkOfEdgesIntoPrivateMemory(graphPath.toString());

		// Run kernel 1
		// for (int i = 0; i < 10; i++) {
		long startTime = System.currentTimeMillis();
		k1.compute(graphPath.toString());
		long endTime = System.currentTimeMillis();

		PCJ.barrier();
		k1.myGraphChunks = null;
		k1.xoffForAllLocal = null;
		k1.xadjForAllLocal = null;
		System.gc();

		if (PCJ.myId() == 0) {
			System.out.println(PCJ.myId() + " of " + PCJ.threadCount()
					+ " tasks -- Kernel 1 PCJ 4 done --- Load all -- test " + 0
					+ " : " + (endTime - startTime) / 1000 + "s");
		}
		PCJ.barrier();
		// }
		if (PCJ.myId() == 0)
			System.out.println("Kernel 1 PCJ 4  test finished");

		// Kernel 2
		int[] graphRoots = KernelsHelper.readToArrayBFSGraphRoots(
				graphPath.toString() + "_root", 5);
		k2.compute(graphRoots);

	}

	private void runReadFile() {
		Kernel1ReadFile k1 = new Kernel1ReadFile(this);
		Kernel2 k2 = new Kernel2(this, k1);

		PCJ.barrier();

		final Path graphPath = Paths.get(System.getProperty("graph.path"));
		if (PCJ.myId() == 0)
			System.out.println(PCJ.myId()
					+ ": Running Graph500 for graph from file "
					+ graphPath.toString());

		// Run kernel 1
		// for (int i = 0; i < 10; i++) {
		long startTime = System.currentTimeMillis();
		k1.compute(graphPath.toString());
		long endTime = System.currentTimeMillis();

		PCJ.barrier();
		if (PCJ.myId() == 0) {
			System.out.println(PCJ.myId() + " of " + PCJ.threadCount()
					+ " tasks -- Kernel 1 PCJ 4 done --- Read file -- test "
					+ 0 + " : " + (endTime - startTime) / 1000 + "s");
		}
		PCJ.barrier();
		// }
		if (PCJ.myId() == 0)
			System.out.println("Kernel 1 PCJ 4  test finished");

		// Kernel 2
		int[] graphRoots = KernelsHelper.readToArrayBFSGraphRoots(
				graphPath.toString() + "_root", 5);
		k2.compute(graphRoots);

	}
}
