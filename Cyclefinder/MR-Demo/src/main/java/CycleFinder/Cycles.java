package CycleFinder;

import org.apache.commons.lang.StringUtils;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Cycles extends Configured implements Tool {

	public enum CYCLES{K_CYCLE_COUNT};

	private static final Logger logger = LogManager.getLogger(Cycles.class);

	public static class AdjObj{

		String adjList ="";
		ArrayList<String> fromUsers = new ArrayList<>();
		boolean activeStatus;

		AdjObj(String val)
		{
			String[] valSplit = val.split(":");
			for(int i=0;i<valSplit.length;i++){

				if(i==0)
				{
					String[] adj = valSplit[i].split("/");
					this.adjList=adj[0];
					if(adj.length>1)
						this.activeStatus= adj[1].equals("active");

				}
				else
				{
					this.fromUsers.add(valSplit[i]);
				}

			}
		}
	}


	public static class CycleMapper extends Mapper<Object, Text, Text, Text> {

		int iterationCount;
		int kCount;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			String itCount = context.getConfiguration().get("Itr");
			iterationCount = Integer.parseInt(itCount);
			kCount= Integer.parseInt(context.getConfiguration().get("k"));
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			logger.info("in second mapper");
			Configuration confs = context.getConfiguration();
			final StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String valueKey = itr.nextToken();
			String valueVal = itr.nextToken();

			AdjObj adj = new AdjObj(valueVal);

			if(iterationCount==1)
				adj.activeStatus=true;

			if(adj.activeStatus)
			{
				adj.activeStatus=false;
				String[] adjNodes = adj.adjList.split(",");
				for(String s: adjNodes)
				{
					Text outKey = new Text(s);
					String outVal="";
					if(adj.fromUsers.isEmpty())
					{
						outVal="path-"+valueKey;
						context.write(outKey,new Text(outVal));
					}
					else
					{
						for(String node: adj.fromUsers)
						{
							int cycl=0;
							if(node.contains(s))
							{

								String[] temp = node.split("->");
								int userCount=(temp.length-1)+2;
								String[] extraPath = node.substring(0, node.indexOf(s)).split("->");
								cycl = userCount - (extraPath.length-1);
							}
							if(cycl>0)
							{
								if (cycl == iterationCount && kCount==cycl){
									context.getCounter(CYCLES.K_CYCLE_COUNT).increment(1);
								}


							}
							else
							{
								outVal="path-"+node+"->"+valueKey;
								context.write(outKey,new Text(outVal));
							}
						}
					}
				}
			}

			String users = "";
			String adjVal="";
			String activeOrNot=adj.activeStatus? "active":"not";
			if(!adj.fromUsers.isEmpty())
			{
				for(String s :adj.fromUsers)
				{
					users+=":"+s;
				}

				adjVal = adj.adjList+"/"+activeOrNot+users;
			}
			else
			{
				adjVal= adj.adjList+"/"+activeOrNot;
			}

			context.write(new Text(valueKey),new Text(adjVal));

		}
	}

	public static class CycleReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			logger.info("in second reducer");

			HashSet<String> userNodes = new HashSet<>();
			AdjObj adj=null;
			boolean status = false;

			for(Text v: values)
			{
				if(v.toString().contains("path-"))
				{
					String p= v.toString().replace("path-","");

					userNodes.add(p);
					status=true;



				}
				else
				{
					adj = new AdjObj(v.toString());
				}
			}

			if(adj!=null) {
				adj.activeStatus=status;
				for (String s : userNodes) {
					for (int i = 0; i < adj.fromUsers.size(); i++) {
						if (s.contains(adj.fromUsers.get(i)))
							adj.fromUsers.remove(i);
					}

					adj.fromUsers.add(s);
				}

				String users = "";
				String adjVal = "";
				String activeOrNot = adj.activeStatus ? "active" : "not";
				if (!adj.fromUsers.isEmpty()) {
					for (String s : adj.fromUsers) {
						users += ":" + s;
					}

					adjVal = adj.adjList + "/" + activeOrNot + users;
				} else {
					adjVal = adj.adjList + "/" + activeOrNot;
				}

				context.write(key, new Text(adjVal));
			}

		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf1 = this.getConf();

		// Job has a Mapper and a Reducer
		final Job job = Job.getInstance(conf1, "Cycles Finder");

		job.setJarByClass(Cycles.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		jobConf.set("Itr",""+Integer.parseInt(args[2]));
		jobConf.set("k",""+Integer.parseInt(args[3]));
		job.setMapperClass(CycleMapper.class);
		job.setReducerClass(CycleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		long count = job.getCounters().findCounter(CYCLES.K_CYCLE_COUNT).getValue();

		logger.info("No. of cycles of length "+args[2]+" = "+count/(Integer.parseInt(args[2])));
		if(Integer.parseInt(args[2])==Integer.parseInt(args[3]))
			System.out.println("No. of cycles of length "+args[2]+" = "+count/(Integer.parseInt(args[2])));

		return 1;

	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir> <iteration> <cycle-length> ");
		}

		try {

			ToolRunner.run(new Cycles(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}