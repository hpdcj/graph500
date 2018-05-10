package g500inpcj;

import java.util.Arrays;

import org.pcj.PCJ;

public class AllTasksBufferWrapper {

	public static final int BUFFER_LENGTH = 1024 * 8;

	private String nameOfReceivedVariable;
	private String nameOfCounterVariable;
	private int[][] toSendPredBuffer;
	private int[] toSendPredIndex;
	private int[] toSendPredPartCounter;

	public AllTasksBufferWrapper(String nameOfReceivedVariable,
			String nameOfCounterVariable) {
		this.toSendPredBuffer = new int[PCJ.threadCount()][BUFFER_LENGTH];
		this.toSendPredIndex = new int[PCJ.threadCount()];
		this.toSendPredPartCounter = new int[PCJ.threadCount()];
		this.nameOfReceivedVariable = nameOfReceivedVariable;
		this.nameOfCounterVariable = nameOfCounterVariable;
	}

	public void send(int taskId, int v) {
		toSendPredBuffer[taskId][toSendPredIndex[taskId]] = v;
		toSendPredIndex[taskId]++;

		if (toSendPredIndex[taskId] == BUFFER_LENGTH) {
			PCJ.put(taskId, nameOfReceivedVariable, toSendPredBuffer[taskId],
					PCJ.myId(), toSendPredPartCounter[taskId]);
			toSendPredIndex[taskId] = 0;
			toSendPredPartCounter[taskId]++;
		}
	}

	public void sendAllRemaining() {
		for (int taskId = 0; taskId < PCJ.threadCount(); taskId++) {
			if (toSendPredIndex[taskId] > 0) {
				PCJ.put(taskId, nameOfReceivedVariable, Arrays.copyOf(
						toSendPredBuffer[taskId], toSendPredIndex[taskId]), PCJ
						.myId(), toSendPredPartCounter[taskId]);
				toSendPredIndex[taskId] = 0;
				toSendPredPartCounter[taskId]++;

			}
			PCJ.put(taskId, nameOfCounterVariable,
					toSendPredPartCounter[taskId], PCJ.myId());
			toSendPredPartCounter[taskId] = 0;
		}
	}
}
