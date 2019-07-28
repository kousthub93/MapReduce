package CycleFinder;

import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;


public class Main  {

	private static final Logger logger = LogManager.getLogger(Main.class);

	public static int status;

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <length of Cycle>");
		}

		try {
			String adjOutput = "s3://spark-cs6240-course/outputAdj";
			//String ssrcOutput = "s3://mapreduce-course/outputFinal";
			//String adjOutput = "/home/rachna/Documents/project/MR-Demo/adjOutput";

			status = ToolRunner.run(new AdjacencyList(), new String[]{args[0],adjOutput});

			String interOutput = "";
			String writeTo ="";

			for (int i = 1; i <=Integer.parseInt(args[2]);i++) {

				if (i == 1) {
					//interOutput = args[1];
					interOutput = adjOutput;
					writeTo = args[1] + "/iteration1";
				} else {
					interOutput = args[1] + "/iteration" + (i - 1);
					writeTo = args[1] + "/iteration" + i;

				}
				ToolRunner.run(new Cycles(), new String[]{interOutput, writeTo, String.valueOf(i),args[2]});

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