package g500inpcj;

import java.util.BitSet;

import org.pcj.PCJ;

public class Kernel2 {

	private static final String PRED_RECV = "receivedPred";
	private static final String PRED_RECV_PARTS = "howManyPartsSent";

	private G500InPCJ storage;
	private Kernel1 k1;

	private long[] pred;
	private int[] current_frontier;
	private int current_frontier_index;
	private int[] next_frontier;
	private int next_frontier_index;

	public Kernel2(G500InPCJ storage, Kernel1 k1) {
		this.storage = storage;
		this.k1 = k1;
	}

	public void compute(int[] graphRoots) {
		long startTime, endTime;

		for (int i = 0; i < graphRoots.length; i++) {
			PCJ.barrier();
			if (PCJ.myId() == 0) {
				System.out.println("Starting BFS for source: " + graphRoots[i]);
			}
			PCJ.barrier();
			startTime = System.currentTimeMillis();

			int initialNextFrontierSize = storage.xoff.length;

			int toSendPredBufferLength = 1024 * 16;

			int receivedPredPartLength = (65536 * 2) / PCJ.threadCount();

			initDataForBFS(toSendPredBufferLength, receivedPredPartLength,
					initialNextFrontierSize);
			bfs(graphRoots[i]);
			endTime = System.currentTimeMillis();

			if (PCJ.myId() == 0) {
				System.out.println(PCJ.myId() + " of " + PCJ.threadCount()
						+ " tasks ---- Kernel 2 done for source "
						+ graphRoots[i] + "! --- " + (endTime - startTime)
						/ 1000 + "s");
			}

			verifyBFS(graphRoots[i]);
			// KernelsHelper.printDummyGraphOutcomes(storage.xadj, storage.xoff,
			// pred);
		}
	}

	private void initDataForBFS(final int toSendPredBufferLength,
			final int receivedPredPartLength, final int initialNextFrontierSize) {
		// bitmap[v] - bit set 1 if vertex v is visited, otherwise 0
		storage.bitmap = new BitSet[PCJ.threadCount()];
		storage.bitmap[PCJ.myId()] = new BitSet(storage.xoff.length);

		pred = new long[storage.xoff.length];

		// First index points task ID the map is received from. Second index
		// points part of send data.
		storage.receivedPred = new int[PCJ.threadCount()][receivedPredPartLength][];

		storage.howManyVerticesInNextFrontier = new int[PCJ.threadCount()];

		storage.howManyPartsSent = new int[PCJ.threadCount()];

		storage.shouldComputeNextLevel = false;

		current_frontier = new int[1];
		current_frontier_index = 0;
		next_frontier = new int[initialNextFrontierSize];
		next_frontier_index = 0;

		storage.numberOfVisitedVertices = new long[PCJ.threadCount()];
		PCJ.barrier();
	}

	public void bfs(final int source) {
		int taskOwnerOfSource = k1.findTaskForGlobalVertexNumber(source);
		if (taskOwnerOfSource == PCJ.myId()) {
			current_frontier[0] = source;
			current_frontier_index++;
			int sourceLocal = k1
					.findLocalVertexNumberForGlobalVertexNumber(source);
			pred[sourceLocal] = -1;
			((BitSet) storage.bitmap[PCJ.myId()]).set(sourceLocal);
		}

		AllTasksBufferWrapper buffer = new AllTasksBufferWrapper(PRED_RECV,
				PRED_RECV_PARTS);
		while (true) {
			for (int index = 0; index < current_frontier_index; index++) {
				// global vertex number (in currentFrontier there are only
				// vertices owned by thread doing this code)
				int vertex1Global = current_frontier[index];
				int vertex1Local = k1
						.findLocalVertexNumberForGlobalVertexNumber(vertex1Global);
				for (long i = getADJIndexFrom(vertex1Local); i < storage.xoff[vertex1Local]; i++) {
					int vertex2Global = (int) storage.xadj[(int) i];

					int taskOwnerOfVertex2Global = k1
							.findTaskForGlobalVertexNumber(vertex2Global);
					if (storage.bitmap[taskOwnerOfVertex2Global] == null
							|| ((BitSet) storage.bitmap[taskOwnerOfVertex2Global])
									.get(k1.findLocalVertexNumberForGlobalVertexNumber(vertex2Global)) == false) {

						if (PCJ.myId() == taskOwnerOfVertex2Global) {
							int vertex2Local = k1
									.findLocalVertexNumberForGlobalVertexNumber(vertex2Global);
							pred[vertex2Local] = vertex1Global;
							next_frontier[next_frontier_index] = vertex2Global;
							next_frontier_index++;
							((BitSet) storage.bitmap[PCJ.myId()])
									.set(vertex2Local);
						} else {
							buffer.send(taskOwnerOfVertex2Global, vertex2Global);
							buffer.send(taskOwnerOfVertex2Global, vertex1Global);
						}
					}
				}
			}

			// Lets send all remaining messages
			buffer.sendAllRemaining();

			int howManyPartsSentCounter = PCJ.waitFor(PRED_RECV_PARTS, 0);
			while (howManyPartsSentCounter < PCJ.threadCount()) {
				visitVerticesFromReceivedPred(false);
				howManyPartsSentCounter = PCJ.waitFor(PRED_RECV_PARTS, 0);
			}
			PCJ.monitor(PRED_RECV_PARTS);

			int howManyChunksWeShouldWaitFor = 0;
			for (int i = 0; i < storage.howManyPartsSent.length; i++) {
				howManyChunksWeShouldWaitFor += storage.howManyPartsSent[i];
			}

			int receivedPredCounter = PCJ.waitFor(PRED_RECV, 0);
			while (receivedPredCounter < howManyChunksWeShouldWaitFor) {
				visitVerticesFromReceivedPred(false);
				receivedPredCounter = PCJ.waitFor(PRED_RECV, 0);
			}
			PCJ.monitor(PRED_RECV);
			visitVerticesFromReceivedPred(true);

			for (int i = 0; i < storage.receivedPred.length; i++) {
				for (int j = 0; j < storage.receivedPred[i].length; j++) {
					if (storage.receivedPred[i][j] != null) {
						storage.receivedPred[i][j] = null;
					} else {
						break;
					}
				}
			}

			PCJ.putLocal("howManyVerticesInNextFrontier", next_frontier_index,
					PCJ.myId());
			if (PCJ.myId() != 0) {
				PCJ.put(0, "howManyVerticesInNextFrontier",
						storage.howManyVerticesInNextFrontier[PCJ.myId()],
						PCJ.myId());
			} else {
				// Task with Id 0 checks if bfs should be continued.
				int howManyVerticesInNextFrontierIsDonePutCounter = PCJ
						.waitFor("howManyVerticesInNextFrontier", 0);
				while (howManyVerticesInNextFrontierIsDonePutCounter < PCJ
						.threadCount()) {
					storage.shouldComputeNextLevel = checkIfBFSShouldBeContinued();
					if (storage.shouldComputeNextLevel == true) {
						PCJ.broadcast("shouldComputeNextLevel", true);
						break;
					}
					howManyVerticesInNextFrontierIsDonePutCounter = PCJ
							.waitFor("howManyVerticesInNextFrontier", 0);
				}

				if (storage.shouldComputeNextLevel == false) {
					storage.shouldComputeNextLevel = checkIfBFSShouldBeContinued();
					if (storage.shouldComputeNextLevel == true) {
						PCJ.broadcast("shouldComputeNextLevel", true);
					} else {
						PCJ.broadcast("shouldComputeNextLevel", false);
					}
				}
			}

			PCJ.waitFor("shouldComputeNextLevel");

			if (storage.shouldComputeNextLevel == true) {
				current_frontier = new int[next_frontier_index];
				current_frontier_index = next_frontier_index;
				for (int idx = 0; idx < current_frontier_index; idx++) {
					current_frontier[idx] = next_frontier[idx];
				}
				next_frontier_index = 0;

				storage.howManyVerticesInNextFrontier = new int[PCJ
						.threadCount()];
				storage.shouldComputeNextLevel = false;

				for (int i = 0; i < PCJ.threadCount(); i++) {
					PCJ.put(i, "bitmap", storage.bitmap[PCJ.myId()], PCJ.myId());
				}

				PCJ.waitFor("bitmap", PCJ.threadCount());
				if (PCJ.myId() == 0) {
					PCJ.waitFor("howManyVerticesInNextFrontier",
							PCJ.threadCount());
				}

			} else {
				if (PCJ.myId() == 0) {
					PCJ.waitFor("howManyVerticesInNextFrontier",
							PCJ.threadCount());
				}

				break;
			}
		}

	}

	private long getADJIndexFrom(final int localVertexNumber) {
		if (localVertexNumber > 0) {
			return storage.xoff[localVertexNumber - 1];
		} else {
			return 0;
		}
	}

	private boolean checkIfBFSShouldBeContinued() {
		for (int i = 0; i < storage.howManyVerticesInNextFrontier.length; i++) {
			if (storage.howManyVerticesInNextFrontier[i] > 0) {
				return true;
			}
		}
		return false;
	}

	private void visitVerticesFromReceivedPred(boolean last) {
		for (int i = 0; i < storage.receivedPred.length; i++) {
			for (int j = 0; j < storage.receivedPred[i].length; j++) {
				int[] receivedPredArray = storage.receivedPred[i][j];
				if (receivedPredArray != null) {
					if (receivedPredArray.length == AllTasksBufferWrapper.BUFFER_LENGTH
							|| last) {
						for (int k = 0; k < receivedPredArray.length; k = k + 2) {
							int vertex2Global = receivedPredArray[k];
							int vertex1Global = receivedPredArray[k + 1];
							int vertex2Local = k1
									.findLocalVertexNumberForGlobalVertexNumber(vertex2Global);
							if (vertex2Global == 0 && vertex1Global == 0) {
								break;
							}
							if (((BitSet) storage.bitmap[PCJ.myId()])
									.get(vertex2Local) == false) {
								pred[vertex2Local] = vertex1Global;
								next_frontier[next_frontier_index] = vertex2Global;
								next_frontier_index++;
								((BitSet) storage.bitmap[PCJ.myId()])
										.set(vertex2Local);
							}
						}
						storage.receivedPred[i][j] = new int[0];
					}
				} else {
					break;
				}
			}
		}
	}

	private void verifyBFS(int graphRoots) {
		PCJ.barrier();
		PCJ.monitor("numberOfVisitedVertices");
		PCJ.barrier();
		long myNumberOfVisitedVertices = 0;
		BitSet myVerticesBitSet = storage.bitmap[PCJ.myId()];
		for (int i = 0; i < myVerticesBitSet.length(); i++) {
			if (myVerticesBitSet.get(i)) {
				myNumberOfVisitedVertices++;
			}
		}
		// System.out.println(PCJ.myId() + ":myNumberOfVisitedVertices "
		// + myNumberOfVisitedVertices);
		if (PCJ.myId() != 0) {
			PCJ.put(0, "numberOfVisitedVertices", myNumberOfVisitedVertices,
					PCJ.myId());
		} else {
			PCJ.putLocal("numberOfVisitedVertices", myNumberOfVisitedVertices,
					PCJ.myId());
			PCJ.waitFor("numberOfVisitedVertices", PCJ.threadCount());
			long sum = 0;
			for (int i = 0; i < storage.numberOfVisitedVertices.length; i++) {
				sum = sum + storage.numberOfVisitedVertices[i];
				System.out.println(">>>>>>>>>Thread " + i + " visited  "
						+ storage.numberOfVisitedVertices[i]);
			}
			System.out
					.println(":::::::::::::::::::::::::::::::::::::::::::::::::::TOTAL numberOfVisitedVertices for source "
							+ graphRoots + " = " + sum);
		}
	}
}
