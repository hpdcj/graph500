package hadoop;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Input Output output2 number_of_reducers
 */
public class BFSHadoopCustomWritable extends Configured implements Tool {

	public static enum NextLevel {
		numberOfGrayVertices
	}

	public static class Map extends
			Mapper<LongWritable, CustomWritable, LongWritable, CustomWritable> {

		public void map(LongWritable key, CustomWritable v, Context context)
				throws IOException, InterruptedException {
			// Lets have input file as binary - sequnce file..
			// value in format:
			// color distance_from_source parent adjacency_list
			// Ex. for source vertex
			// G 0 MAX 2,4,5,
			// Ex. for normal vertex
			// W MAX MAX 2,4,67,
			// key is vertex id

			if (v.getColor() == 'G') {
				// Vertex v has Gray color so ..
				for (final long v2Id : v.getAdjacencyList()) {
					// .. for every adjacent vertex emit: color distance parent
					CustomWritable v2 = new CustomWritable('G',
							v.getDistance() + 1, key.get());
					context.write(new LongWritable(v2Id), v2);
				}
				v.setColor('B');
			}
			// For vertex emit: color distance parent adjacency_list
			context.write(key, v);
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, CustomWritable, LongWritable, CustomWritable> {

		public void reduce(LongWritable key, Iterable<CustomWritable> values,
				Context context) throws IOException, InterruptedException {

			final CustomWritable reduced = new CustomWritable('W',
					Integer.MAX_VALUE, Integer.MAX_VALUE);

			for (CustomWritable v : values) {

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

			context.write(key, reduced);

			if (reduced.getColor() == 'G')
				context.getCounter(NextLevel.numberOfGrayVertices)
						.increment(1L);

		}
	}

	public int run(String[] args) throws Exception {

		String input, output;
		long numberOfGrayVertices = 1;
		int numberOfReducersTasks = Integer.valueOf(args[3]);
		int i = 0;

		while (numberOfGrayVertices > 0) {

			Job job = Job.getInstance();
			job.setJarByClass(BFSHadoopCustomWritable.class);
			job.setJobName("BFS Hadoop Seq");
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(CustomWritable.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(CustomWritable.class);

			job.setNumReduceTasks(numberOfReducersTasks);

			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			if (i == 0) {
				input = args[0];
			} else {
				input = args[1] + i;
			}
			output = args[1] + (i + 1);

			FileInputFormat.setInputPaths(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));

			job.waitForCompletion(true);

			numberOfGrayVertices = job.getCounters()
					.findCounter(NextLevel.numberOfGrayVertices).getValue();
			i++;
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(), new BFSHadoopCustomWritable(),
				args);
		long endTime = System.currentTimeMillis();
		long time = endTime - startTime;

		long bfsDoneInS = time / 1000;

		final BufferedWriter writer = new BufferedWriter(
				new FileWriter(args[2]));
		writer.write("BFS Done in: " + bfsDoneInS + "s");
		writer.newLine();
		writer.close();

		System.exit(exitCode);
	}
}