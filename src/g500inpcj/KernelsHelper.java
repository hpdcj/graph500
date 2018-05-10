package g500inpcj;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.pcj.PCJ;

public class KernelsHelper {

	public static int[] readToArrayBFSGraphRoots(
			final String graphRootsFilePath, final int howManyRootsToRead) {

		int[] graphRoots = new int[howManyRootsToRead];

		byte[] buffer = new byte[8];
		try (final DataInputStream data = new DataInputStream(
				new BufferedInputStream(new FileInputStream(graphRootsFilePath)))) {
			for (int i = 0; i < graphRoots.length; i++) {
				try {
					// little endian
					int numberOfBytesRead = data.read(buffer);
					if (numberOfBytesRead < 0) {
						// EOF
						break;
					}
					int pos = 0;
					graphRoots[i] = (buffer[pos++] & 0xFF)
							| (buffer[pos++] & 0xFF) << 8
							| (buffer[pos++] & 0xFF) << 16
							| (buffer[pos++] & 0xFF) << 24
							| (buffer[pos++] & 0xFF) << 32
							| (buffer[pos++] & 0xFF) << 40
							| (buffer[pos++] & 0xFF) << 48
							| (buffer[pos++] & 0xFF) << 56;

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return graphRoots;
	}

	public static void printDummyGraphOutcomes(final long[] xadj,
			final long[] xoff, final long[] pred) {
		PCJ.barrier();
		if (PCJ.myId() == 0) {
			System.out.println("---------XADJ----------");
		}
		PCJ.barrier();
		printWithThreadOrder(xadj);

		PCJ.barrier();
		if (PCJ.myId() == 0) {
			System.out.println("---------XOFF----------");
		}
		PCJ.barrier();
		printWithThreadOrder(xoff);

		PCJ.barrier();
		if (PCJ.myId() == 0) {
			System.out.println("---------PRED----------");
		}
		PCJ.barrier();
		printWithThreadOrder(pred);
	}

	public static void printWithThreadOrder(final long[] array) {
		StringBuilder s = new StringBuilder();
		s.append("[");
		for (int i = 0; i < array.length; i++) {
			s.append(array[i]);
			if (i < (array.length - 1)) {
				s.append(", ");
			}
		}
		s.append("]");
		for (int i = 0; i < PCJ.threadCount(); i++) {
			if (PCJ.myId() == i) {
				System.out.println(s.toString());
			}
			PCJ.barrier();
		}
	}

	public static void printWithThreadOrder(final int[] array) {
		StringBuilder s = new StringBuilder();
		s.append("[");
		for (int i = 0; i < array.length; i++) {
			s.append(array[i]);
			if (i < (array.length - 1)) {
				s.append(", ");
			}
		}
		s.append("]");
		for (int i = 0; i < PCJ.threadCount(); i++) {
			if (PCJ.myId() == i) {
				System.out.println(s.toString());
			}
			PCJ.barrier();
		}
	}

	public static void printWithThreadOrder(final long[][] array) {
		StringBuilder s = new StringBuilder();
		for (int k = 0; k < array.length; k++) {
			s.append("[");
			for (int i = 0; i < array[k].length; i++) {
				s.append(array[k][i]);
				if (i < (array[k].length - 1)) {
					s.append(", ");
				}
			}
			s.append("]");
		}

		for (int i = 0; i < PCJ.threadCount(); i++) {
			if (PCJ.myId() == i) {
				System.out.println(PCJ.myId()
						+ "---------xoffGatheredFromTasks----------");
				System.out.println(s.toString());
			}
			PCJ.barrier();
		}
	}

	public static void printWithThreadOrder(final int[][] array) {
		StringBuilder s = new StringBuilder();
		for (int k = 0; k < array.length; k++) {
			s.append("[");
			for (int i = 0; i < array[k].length; i++) {
				s.append(array[k][i]);
				if (i < (array[k].length - 1)) {
					s.append(", ");
				}
			}
			s.append("]");
		}

		for (int i = 0; i < PCJ.threadCount(); i++) {
			if (PCJ.myId() == i) {
				System.out.println(PCJ.myId()
						+ "---------xoffGatheredFromTasks----------");
				System.out.println(s.toString());
			}
			PCJ.barrier();
		}
	}

	public static long shiftByteOrder(byte[][] graph, int idx, int pos) {
		return (graph[idx][pos++] & 0xFF) | (graph[idx][pos++] & 0xFF) << 8
				| (graph[idx][pos++] & 0xFF) << 16
				| (graph[idx][pos++] & 0xFF) << 24
				| (graph[idx][pos++] & 0xFF) << 32
				| (graph[idx][pos++] & 0xFF) << 40
				| (graph[idx][pos++] & 0xFF) << 48
				| (graph[idx][pos] & 0xFF) << 56;
	}

	public static long shiftByteOrder(byte[] buff, int pos) {
		return (buff[pos++] & 0xFF) | (buff[pos++] & 0xFF) << 8
				| (buff[pos++] & 0xFF) << 16 | (buff[pos++] & 0xFF) << 24
				| (buff[pos++] & 0xFF) << 32 | (buff[pos++] & 0xFF) << 40
				| (buff[pos++] & 0xFF) << 48 | (buff[pos] & 0xFF) << 56;
	}

	public static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l
					+ " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}
}
