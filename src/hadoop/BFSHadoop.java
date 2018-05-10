package hadoop;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Input Output output2 number_of_reducers
 */
public class BFSHadoop extends Configured implements Tool {

	public static enum NextLevel {
		numberOfGrayVertices
	}

	public static class Map extends Mapper<LongWritable, Text, LongWritable, BytesWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// For now lets have input file as text (maybe later as a binary)..
			// value == line of text in format:
			// vertex_id color distance_from_source parent adjacency_list
			// Ex. for source vertex
			// 0 G 0 MAX 2,4,5,
			// Ex. for normal vertex
			// 3 W MAX MAX 2,4,67,

			// Create vertex from single line of text
			final Vertex v1 = new Vertex(value.toString());

			if (v1.getColor() == 'G') {
				// Vertex v1 has Gray color so ..
				for (final long v2Id : v1.getAdjacencyList()) {
					// .. for every adjacent vertex emit: color distance parent
					Vertex v2 = new Vertex(v2Id, 'G', v1.getDistance() + 1, v1.getId());
					context.write(new LongWritable(v2.getId()), v2.getVertexInfoAsBytesWritable());
				}
				v1.setColor('B');
			}
			// For vertex emit: color distance parent adjacency_list
			context.write(new LongWritable(v1.getId()), v1.getVertexInfoAsBytesWritable());
		}
	}

	public static class Reduce extends Reducer<LongWritable, BytesWritable, LongWritable, Text> {

		public void reduce(LongWritable key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			final Vertex reduced = new Vertex(key.get(), 'W', Integer.MAX_VALUE, Integer.MAX_VALUE);

			for (BytesWritable value : values) {
				Vertex v = new Vertex(value);

				if (v.getAdjacencyList().length > 0) {
					reduced.setAdjacencyList(v.getAdjacencyList());
				}

				if (v.getDistance() < reduced.getDistance()) {
					reduced.setDistance(v.getDistance());
					reduced.setParent(v.getParent());
				}

				if (reduced.getColor() > v.getColor()) {
					// save the darkest color
					reduced.setColor(v.getColor());
				}
			}

			context.write(new LongWritable(reduced.getId()), reduced.getVertexInfoAsText());

			if (reduced.getColor() == 'G')
				context.getCounter(NextLevel.numberOfGrayVertices).increment(1L);

		}
	}

	public int run(String[] args) throws Exception {

		String input, output;
		long numberOfGrayVertices = 1;
		int numberOfReducersTasks = Integer.valueOf(args[3]);
		int i = 0;

		while (numberOfGrayVertices > 0) {

			Job job = Job.getInstance();
			job.setJarByClass(BFSHadoop.class);
			job.setJobName("BFS Hadoop");
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(numberOfReducersTasks);

			if (i == 0) {
				input = args[0];
			} else {
				input = args[1] + i;
			}
			output = args[1] + (i + 1);

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

			numberOfGrayVertices = job.getCounters().findCounter(NextLevel.numberOfGrayVertices).getValue();
			i++;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(), new BFSHadoop(), args);
		long endTime = System.currentTimeMillis();
		long time = endTime - startTime;

		long bfsDoneInS = time / 1000;

		final BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]));
		writer.write("BFS Done in: " + bfsDoneInS + "s");
		writer.newLine();
		writer.close();

		System.exit(exitCode);
	}
}