package hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomWritable implements Writable {

	private long parent;

	private long[] adjacencyList;

	// W, G, B
	private char color;

	// private long id;

	private long distance;

	public CustomWritable() {
		this.color = 'W';
		this.distance = Long.MAX_VALUE;
		this.parent = Long.MAX_VALUE;
		this.adjacencyList = new long[0];
	}

	public CustomWritable(char color, long distance, long parent) {
		this.color = color;
		this.distance = distance;
		this.parent = parent;
		this.adjacencyList = new long[0];
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// color distance_from_source parent adjacency_list
		color = in.readChar();
		distance = in.readLong();
		parent = in.readLong();
		int length = in.readInt();
		adjacencyList = new long[length];
		for (int i = 0; i < length; i++) {
			adjacencyList[i] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// color distance_from_source parent adjacency_list
		out.writeChar(color);
		out.writeLong(distance);
		out.writeLong(parent);
		out.writeInt(adjacencyList.length);
		for (long l : adjacencyList) {
			out.writeLong(l);
		}
	}

	public long getParent() {
		return parent;
	}

	public long[] getAdjacencyList() {
		return adjacencyList;
	}

	public char getColor() {
		return color;
	}

	public long getDistance() {
		return distance;
	}

	public void setParent(long parent) {
		this.parent = parent;
	}

	public void setAdjacencyList(long[] adjacencyList) {
		this.adjacencyList = adjacencyList;
	}

	public void setColor(char color) {
		this.color = color;
	}

	public void setDistance(long distance) {
		this.distance = distance;
	}

	@Override
	public String toString() {
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
		return sb.toString();
	}
}
