package hadoop;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/*
 * Class responsible for converting graph taken from Kronecker Graph Generator Graph500 Benchmark into Hadoop input file.
 * Input graph is binary file - edge list.
 * Output is a text file in format:
 * vertex_id color distance_from_source parent adjacency_list
 *  Ex. for source vertex
 *	0	G 0 MAX 2,4,5,
 *  Ex. for normal vertex
 *  3	W MAX MAX 2,4,67,
 *  
 *  
 *  Program takes 3 parameters:
 *  args[0] - path to the edge list file (file with sources has the same path and name, buts ends with '_root')
 *  args[1] - name of generated file
 *  args[2] - number of generated files parts (named with suffix '_XXX_parts_YYY')
 */
public class BFSHadoopGraphConverter {

	public static void main(String[] args) {

		if (args.length < 3) {
			System.out.println("Usage: graph_path output_path number_of_parts");
			System.exit(1);
		}
		// path to file with edge lists
		String path = args[0];
		String path_roots = args[0] + "_root";
		String output = args[1];
		int numberOfGeneratedParts = Integer.parseInt(args[2]);
		int maxBytesMapable = 512000;

		long maxVertexL = findMaxVertex(maxBytesMapable, path);
		int maxVertexI = safeLongToInt(maxVertexL);

		System.out.println("MAX:" + maxVertexI);

		long[] degrees = findDegrees(maxBytesMapable, maxVertexI, path);

		long[][] adjacencyList = new long[degrees.length][];
		int[] where = new int[degrees.length];
		for (int i = 0; i < adjacencyList.length; i++) {
			adjacencyList[i] = new long[safeLongToInt(degrees[i])];
		}

		// compute adjacency list
		byte[] buffer = new byte[maxBytesMapable];
		int pos = 0;
		try (final DataInputStream data = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
			while (true) {
				try {
					// little endian
					int numberOfBytesRead = data.read(buffer);
					if (numberOfBytesRead < 0) {
						// EOF
						break;
					}
					while (pos < numberOfBytesRead) {

						long i = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						long j = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						if (i != j) { /* Skip self-edges. */
							int iI = safeLongToInt(i);
							int jI = safeLongToInt(j);
							adjacencyList[iI][where[iI]++] = j;
							adjacencyList[jI][where[jI]++] = i;
						}
					}
					pos = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		Set<Integer> sources = findSources(maxBytesMapable, path_roots);

		try {
			boolean needSource = true;
			int avgNumberOfVerticesOnFile = adjacencyList.length / numberOfGeneratedParts;
			int left = adjacencyList.length - numberOfGeneratedParts * avgNumberOfVerticesOnFile;
			int from, to;
			for (int p = 0; p < numberOfGeneratedParts; p++) {
				final BufferedWriter writer = new BufferedWriter(
						new FileWriter(output + "_" + numberOfGeneratedParts + "_parts_" + p));
				int numberOfVerticesOnPart = avgNumberOfVerticesOnFile;
				if (left > p) {
					numberOfVerticesOnPart++;
					from = p * numberOfVerticesOnPart;
				} else {
					from = p * numberOfVerticesOnPart + left;
				}
				to = from + numberOfVerticesOnPart - 1;

				for (int v = from; v <= to; v++) {
					if (needSource && sources.contains(Integer.valueOf(v))) {
						writer.write(v + "\tG 0 MAX ");
						needSource = false;
					} else {
						writer.write(v + "\tW MAX MAX ");
					}
					for (int j = 0; j < adjacencyList[v].length; j++) {
						writer.write(adjacencyList[v][j] + ",");
					}
					writer.newLine();
				}
				writer.close();

			}

			if (needSource) {
				throw new RuntimeException("No source - no vertex with G color");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static Set<Integer> findSources(int maxBytesMapable, String path_roots) {
		Set<Integer> sources = new HashSet<Integer>();

		byte[] buffer = new byte[maxBytesMapable];
		int pos = 0;
		try (final DataInputStream data = new DataInputStream(
				new BufferedInputStream(new FileInputStream(path_roots)))) {
			while (true) {
				try {
					// little endian
					int numberOfBytesRead = data.read(buffer);
					if (numberOfBytesRead < 0) {
						// EOF
						break;
					}
					while (pos < numberOfBytesRead) {

						long v = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						sources.add(Integer.valueOf(safeLongToInt(v)));

					}
					pos = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return sources;
	}

	private static long[] findDegrees(int maxBytesMapable, int maximumVertex, String path) {

		long[] degrees = new long[maximumVertex + 1];

		byte[] buffer = new byte[maxBytesMapable];
		int pos = 0;
		try (final DataInputStream data = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
			while (true) {
				try {
					// little endian
					int numberOfBytesRead = data.read(buffer);
					if (numberOfBytesRead < 0) {
						// EOF
						break;
					}
					while (pos < numberOfBytesRead) {

						long i = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						long j = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						if (i != j) { /* Skip self-edges. */
							++degrees[safeLongToInt(i)];
							++degrees[safeLongToInt(j)];
						}
					}
					pos = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return degrees;
	}

	private static long findMaxVertex(int maxBytesMapable, String path) {
		long maximumVertex = 0;

		byte[] buffer = new byte[maxBytesMapable];
		int pos = 0;
		try (final DataInputStream data = new DataInputStream(new BufferedInputStream(new FileInputStream(path)))) {
			while (true) {
				try {
					// little endian
					int numberOfBytesRead = data.read(buffer);
					if (numberOfBytesRead < 0) {
						// EOF
						break;
					}
					while (pos < numberOfBytesRead) {

						long v = (buffer[pos++] & 0xFF) | (buffer[pos++] & 0xFF) << 8 | (buffer[pos++] & 0xFF) << 16
								| (buffer[pos++] & 0xFF) << 24 | (buffer[pos++] & 0xFF) << 32
								| (buffer[pos++] & 0xFF) << 40 | (buffer[pos++] & 0xFF) << 48
								| (buffer[pos++] & 0xFF) << 56;

						if (v > maximumVertex) {
							maximumVertex = v;
						}
					}
					pos = 0;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return maximumVertex;
	}

	private static int safeLongToInt(long l) {
		if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
			throw new IllegalArgumentException(l + " cannot be cast to int without changing its value.");
		}
		return (int) l;
	}

}
