import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Scanner;

public class DummyToRealFileConverter {

	private static String TEXT_GRAPH = "graph";

	public static void main(String[] args) throws IOException {
		createDummyGraphReal(TEXT_GRAPH);
		createDummyGraphRealRoot(TEXT_GRAPH);
	}

	private static void createDummyGraphRealRoot(String graphName)
			throws IOException {
		File fileInput = new File(graphName + "_root");
		FileOutputStream outputStream = new FileOutputStream(graphName
				+ "_real_root");

		try (Scanner sc = new Scanner(fileInput)) {
			while (sc.hasNextLine()) {
				Scanner sc2 = new Scanner(sc.nextLine());
				long v1 = sc2.nextInt();
				sc2.close();

				ByteBuffer b = ByteBuffer.allocate(8);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putLong(v1);

				outputStream.write(b.array());

			}
		} catch (FileNotFoundException e) {
			System.err.println("File with name '" + fileInput.getAbsolutePath()
					+ "' not found!");
			System.exit(1);
		}
		outputStream.close();

	}

	private static void createDummyGraphReal(String graphName)
			throws IOException {
		File fileInput = new File(graphName);
		FileOutputStream outputStream = new FileOutputStream(graphName
				+ "_real");

		try (Scanner sc = new Scanner(fileInput)) {
			while (sc.hasNextLine()) {
				Scanner sc2 = new Scanner(sc.nextLine());
				long v1 = sc2.nextInt();
				long v2 = sc2.nextInt();
				System.out.println(v1 + " " + v2);
				sc2.close();

				ByteBuffer b = ByteBuffer.allocate(16);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putLong(v1);
				b.putLong(v2);

				outputStream.write(b.array());

			}
		} catch (FileNotFoundException e) {
			System.err.println("File with name '" + fileInput.getAbsolutePath()
					+ "' not found!");
			System.exit(1);
		}
		outputStream.close();
	}
}
