package SingleSourceTwitter;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

public class Main  {

	private static final Logger logger = LogManager.getLogger(Main.class);

	public static int status;

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Four arguments required:\n<input-dir> <output-dir> <Source Vertex> <levels>");
		}

		try {
			String adjOutput = "s3://kous-mr/outputAdj";
			String ssrcOutput = "s3://kous-mr/outputConnections";
			//String adjOutput = "/home/rachna/Documents/cs6240/Project/cs6240_project_RK/MR-Demo/adjOutput";
			//String ssrcOutput = "/home/rachna/Documents/cs6240/Project/cs6240_project_RK/MR-Demo/finalOutput";

			status = ToolRunner.run(new AdjacencyList(), new String[]{args[0],adjOutput});

			String interOutput = "";
			String writeTo ="";
			String finalOutput ="";

			for (int i = 1; i <=Integer.parseInt(args[3]);i++) {

				if (i == 1) {
				    //interOutput = args[1];
					interOutput = adjOutput;
					writeTo = args[1] + "/iteration1";
					finalOutput = ssrcOutput+"/iteration1";
				} else {
					interOutput = args[1] + "/iteration" + (i - 1);
					writeTo = args[1] + "/iteration" + i;
					finalOutput = ssrcOutput+ "/iteration"+i;

				}
				ToolRunner.run(new SingleSource(), new String[]{interOutput, writeTo, finalOutput, args[2]});

				/*Configuration confFile = new Configuration();
				FileSystem fs = FileSystem.get(new URI("s3://mapreduce-course"),confFile);
				fs.delete(new Path(interOutput));*/

			}

			logger.info("input path is====="+writeTo);




        } catch (final Exception e) {
			logger.error("", e);
		}
	}
}