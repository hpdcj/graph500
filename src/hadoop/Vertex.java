package hadoop;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class Vertex {

	private long parent;

	private long[] adjacencyList;

	// W, G, B
	private char color;

	private long id;

	private long distance;

	public Vertex(final String textLine) {
		final String[] keyAndValue = textLine.split("\t");
		// keyAndValue[0] is key, keyAndValue[1] is value
		id = Long.parseUnsignedLong(keyAndValue[0]);
		final String[] splitted = keyAndValue[1].split(" ");
		color = splitted[0].charAt(0);
		if (splitted[1].equals("MAX")) {
			distance = Integer.MAX_VALUE;
		} else {
			distance = Integer.parseUnsignedInt(splitted[1]);
		}
		if (splitted[2].equals("MAX")) {
			parent = Integer.MAX_VALUE;
		} else {
			parent = Long.parseUnsignedLong(splitted[2]);
		}
		String[] adjacencyListAsStrings;
		if (splitted.length > 3) {
			adjacencyListAsStrings = splitted[3].split(",");
		} else {
			adjacencyListAsStrings = new String[0];
		}
		adjacencyList = new long[adjacencyListAsStrings.length];
		for (int i = 0; i < adjacencyListAsStrings.length; i++) {
			adjacencyList[i] = Long
					.parseUnsignedLong(adjacencyListAsStrings[i]);
		}
	}

	public Vertex(long id, char color, long distance, long parent) {
		this.id = id;
		this.color = color;
		this.distance = distance;
		this.parent = parent;
		adjacencyList = new long[0];
	}

	public Vertex(BytesWritable bytesWritable) throws IOException {
		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(
				bytesWritable.getBytes());
		DataInputStream dataInputStream = new DataInputStream(byteInputStream);
		this.color = dataInputStream.readChar();
		this.distance = dataInputStream.readLong();
		this.parent = dataInputStream.readLong();
		int adjacencyListLength = (bytesWritable.getLength() - Character.BYTES
				- Long.BYTES - Long.BYTES)
				/ Long.BYTES;
		this.adjacencyList = new long[adjacencyListLength];
		for (int i = 0; i < adjacencyListLength; i++) {
			adjacencyList[i] = dataInputStream.readLong();
		}
	}

	public void setParent(long parent) {
		this.parent = parent;
	}

	public long getParent() {
		return parent;
	}

	public void setDistance(long distance) {
		this.distance = distance;
	}

	public long getDistance() {
		return distance;
	}

	public void setAdjacencyList(long[] adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public void setColor(final char color) {
		this.color = color;
	}

	public char getColor() {
		return color;
	}

	public long[] getAdjacencyList() {
		return adjacencyList;
	}

	public long getId() {
		return id;
	}

	public BytesWritable getVertexInfoAsBytesWritable() throws IOException {
		// return all information: color distance parent adjacency_list
		ByteArrayOutputStream emittedByteStream = new ByteArrayOutputStream(
				Character.BYTES + Long.BYTES + Long.BYTES
						+ adjacencyList.length * Long.BYTES);
		DataOutputStream emittedDataStream = new DataOutputStream(
				emittedByteStream);
		emittedDataStream.writeChar(color);
		emittedDataStream.writeLong(distance);
		emittedDataStream.writeLong(parent);
		for (long v : adjacencyList) {
			emittedDataStream.writeLong(v);
		}
		emittedDataStream.close();
		return new BytesWritable(emittedByteStream.toByteArray());
	}

	public Text getVertexInfoAsText() {
		StringBuilder sb = new StringBuilder();
		sb.append(color);
		sb.append(" ");
		if (distance >= Integer.MAX_VALUE) {
			sb.append("MAX");
		} else {
			sb.append(distance);
		}
		sb.append(" ");
		if (parent >= Integer.MAX_VALUE) {
			sb.append("MAX");
		} else {
			sb.append(parent);
		}
		sb.append(" ");
		for (long v : this.adjacencyList) {
			sb.append(v);
			sb.append(",");
		}
		return new Text(sb.toString());
	}
}