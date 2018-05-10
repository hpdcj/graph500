package hadoop;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;

/*
 * This class converts a text file in parts (generated from BFSHadoopGraphConverter) to the sequence file.
 */
public class SequenceFileWriteRead {

	public static void main(String[] args) throws IOException {
		// args[0] - is prefix for text file path from local file system ex.
		// converted_graphs/14_kronec/14_kr
		// args[1] - is prefix for seq file path from Hadoop fs ex. 14_kr
		// args[2] - number of input/generated parts

		// writeSeqFile(args[0], args[1], Integer.parseInt(args[2]));

		// /readAllPartsSeqFile(args[1], Integer.parseInt(args[2]));

		// args[3] - the number of part that should be read
		readPartSeqFile(args[1], Integer.parseInt(args[2]),
				Integer.parseInt(args[3]));
	}

	private static void writeSeqFile(String textFilePathPrefix,
			String seqFilePathPrefix, int numberOfInputParts)
			throws IOException {

		for (int p = 0; p < numberOfInputParts; p++) {

			final String suffixPath = "_" + numberOfInputParts + "_parts_" + p;
			final String textFilePath = textFilePathPrefix + suffixPath;
			final String seqFilePath = seqFilePathPrefix + suffixPath;

			Configuration conf = new Configuration();
			FileSystem.get(URI.create(seqFilePath), conf);
			Path path = new Path(seqFilePath);

			String line = "";
			BufferedReader reader = null;
			SequenceFile.Writer writer = null;
			try {
				reader = new BufferedReader(new FileReader(textFilePath));
				writer = SequenceFile.createWriter(conf, Writer.file(path),
						Writer.keyClass(LongWritable.class),
						Writer.valueClass(CustomWritable.class));

				while ((line = reader.readLine()) != null) {
					Vertex v = new Vertex(line);
					LongWritable key = new LongWritable(v.getId());
					CustomWritable value = new CustomWritable(v.getColor(),
							v.getDistance(), v.getParent());
					value.setAdjacencyList(v.getAdjacencyList());
					writer.append(key, value);
				}

			} finally {
				IOUtils.closeStream(writer);
				reader.close();
			}
		}
	}

	private static void readAllPartsSeqFile(String seqFilePathPrefix,
			int numberOfInputParts) throws IOException {

		for (int p = 0; p < numberOfInputParts; p++) {

			final String suffixPath = "_" + numberOfInputParts + "_parts_" + p;
			final String seqFilePath = seqFilePathPrefix + suffixPath;

			System.out.println(seqFilePath);

			Configuration conf = new Configuration();
			Path path = new Path(seqFilePath);

			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(conf, Reader.file(path));

				LongWritable key = new LongWritable();
				CustomWritable value = new CustomWritable('W', 0, 0);

				while (reader.next(key, value)) {
					System.out.println(key + "\t" + value);
				}

			} finally {
				reader.close();
			}
		}
	}

	private static void readPartSeqFile(String seqFilePathPrefix,
			int numberOfInputParts, int numberOfReadPart) throws IOException {

		final String suffixPath = "_" + numberOfInputParts + "_parts_"
				+ numberOfReadPart;
		final String seqFilePath = seqFilePathPrefix + suffixPath;

		//System.out.println(seqFilePath);

		Configuration conf = new Configuration();
		Path path = new Path(seqFilePath);

		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path));

			LongWritable key = new LongWritable();
			CustomWritable value = new CustomWritable('W', 0, 0);

			while (reader.next(key, value)) {
				System.out.println(key + "\t" + value);
			}

		} finally {
			reader.close();
		}
	}
}
