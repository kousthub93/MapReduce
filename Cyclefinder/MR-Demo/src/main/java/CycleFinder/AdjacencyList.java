package CycleFinder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;


public class AdjacencyList extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(AdjacencyList.class);


	public static class GraphMapper extends Mapper<Object, Text, Text, Text> {

		private Text fromKey = new Text();
		private Text toKey = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {


			final StringTokenizer itr = new StringTokenizer(value.toString());
			String user1 = itr.nextToken();
			String user2 = itr.nextToken();

			fromKey.set(user1);
			toKey.set(user2);
			context.write(fromKey, toKey);


		}
	}

	public static class GraphReducer extends Reducer<Text, Text, Text, Text> {


		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			Text nextPath = new Text();

			logger.info("in the first reducer");

			String adj = "";

			HashSet<String> nodes = new HashSet<>();

			for(Text n : values)
			{
				nodes.add(n.toString());
			}

			for (String node : nodes) {
				adj += node + ",";
			}

			String outputVal = adj.substring(0,adj.length()-1);
			Text nextValue = new Text();
			nextPath.set(key);
			nextValue.set(outputVal);

			context.write(nextPath,nextValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		// Job1 has a Mapper and a Reducer
		final Job job1 = Job.getInstance(conf, "Adjacency list");

		job1.setJarByClass(AdjacencyList.class);

		final Configuration jobConf1 = job1.getConfiguration();
		jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

		//job1.setInputFormatClass(NLineInputFormat.class);
		//NLineInputFormat.addInputPath(job1, new Path(args[0]));
		//job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

		//LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
		//FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(GraphMapper.class);
		job1.setReducerClass(GraphReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// Wait until Job1 is completed

		return job1.waitForCompletion(true)?0:1;
	}



	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {

			int status = ToolRunner.run(new AdjacencyList(), args);


		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}