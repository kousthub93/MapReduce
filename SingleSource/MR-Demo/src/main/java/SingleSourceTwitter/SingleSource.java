package SingleSourceTwitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

public class SingleSource extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(SingleSource.class);


	public static class SingleSourceMapper extends Mapper<Object, Text, Text, Text> {

		Text outKey = new Text();
		Text outValue = new Text();

		Text outKey1 = new Text();
		Text outValue1 = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			logger.info("in second mapper");
			Configuration confs = context.getConfiguration();
			String valueKey = "";
			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			if(itr.hasMoreTokens()){
				valueKey = itr.nextToken();
			}

			if(itr.hasMoreTokens()){
				String valueVal = itr.nextToken();

				String sourceV = confs.get("source");
				String[] valuesIn;
				long distance = Long.MAX_VALUE;
				String path=sourceV;
				String adjacency="";

				outKey.set(valueKey);


				if(!valueVal.contains("#"))
				{

					if(sourceV.equals(valueKey))
					{
						distance=0;
						outValue.set(valueVal+"#"+sourceV+"#"+"0");
					}
					else {
						outValue.set(valueVal+"#"+"null"+"#"+String.valueOf(Long.MAX_VALUE));
					}

					adjacency = valueVal;

				}
				else
				{
					valuesIn = valueVal.split("#");
					adjacency = valuesIn[0];
					distance = Long.parseLong(valuesIn[2]);
					path = valuesIn[1];
					outValue.set(valueVal);

				}

				context.write(outKey,outValue);

				for(String vertices: adjacency.split(","))
				{

					String newPath = sourceV;
					long dist = distance;
					if(vertices.equals("null"))
					{
						return;
					}

					if(dist!=Long.MAX_VALUE)
					{
						outKey1.set(vertices);
						dist+=1;
						if(!path.equals(valueKey)) {
							newPath = valueKey + "<-" + path;
						}

						outValue1.set(dist+";"+newPath);


						context.write(outKey1,outValue1);
					}




				}
			}








		}
	}

	public static class SingleSourceReducer extends Reducer<Text, Text, Text, Text> {

		Text outValue = new Text();

		Text outKey2 = new Text();
		Text outValue2 = new Text();

		private MultipleOutputs mos;

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}

		public void setup(Context context){

			mos = new MultipleOutputs(context);
		}

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			Configuration confs1 = context.getConfiguration();

			String outputPath= confs1.get("outputPathStr");
			String adj = "";
			String[] valuesList;
			long dist=0;
			String path="";
			long dMin=Long.MAX_VALUE;

			String distString="";
			String[] distList;
			long distP=0;
			String pathP="";
			String newPath=null;
			String newAdj="";

			for(Text val: values)
			{
				if(val.toString().contains("#"))
				{
					adj = val.toString();
					valuesList = adj.split("#");
					dist = Long.parseLong(valuesList[2]);
					path = valuesList[1];
					if(dist<dMin)
					{
						dMin=dist;
						newPath=path;
					}
					newAdj=valuesList[0];


				}
				else if (val.toString().contains(";"))
				{
					distString=val.toString();
					distList=distString.split(";");
					distP=Long.parseLong(distList[0]);
					pathP=distList[1];
					if(distP<dMin)
					{
						dMin=distP;
						newPath=pathP;
					}

				}
			}

			if(dMin!=dist && dMin!=Long.MAX_VALUE && dMin!=0)
			{
				outKey2.set(key);
				outValue2.set(newPath);
				mos.write("text",outKey2,outValue2,outputPath);
			}

			outValue.set(newAdj+"#"+newPath+"#"+dMin);
			context.write(key,outValue);


		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf1 = this.getConf();
		conf1.set("source",args[3]);
		conf1.set("outputPathStr",args[2]);


		// Job1 has a Mapper and a Reducer
		final Job job = Job.getInstance(conf1, "Single Source Path");

		job.setJarByClass(SingleSource.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		//job.setInputFormatClass(NLineInputFormat.class);
		//NLineInputFormat.addInputPath(job, new Path(args[0]));
		//job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

		job.setMapperClass(SingleSourceMapper.class);
		job.setReducerClass(SingleSourceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);




		return job.waitForCompletion(true)?0:1;

	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir> <final-Output> <Source Vertex>");
		}

		try {

			ToolRunner.run(new SingleSource(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}