package airlineanalysis.airlinesCodeShare;

import airlineanalysis.airlinesZeroStops.AirLinesNameMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AirlinesCodeShareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "list of Airlines having Y codeshare");
		job.setJarByClass(AirlinesCodeShareDriver.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, AirLinesNameMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, AirLinesCodeShareMapper.class);
		job.setReducerClass(AirLinesNamesCodeShareReducer.class);
		Path outputPath = new Path(args[2]);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}