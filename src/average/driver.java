package average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class driver extends Configured implements Tool {
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new driver(),args);
	}
	public int run(String[] args) throws Exception {
	
		Configuration conf = new Configuration();
		if(args.length!=3)
		{
		System.err.println("usage error <in> <out>");
		System.exit(2);
		}
        Job job = new Job(conf, "average age");
        job.setJarByClass(driver.class);
        job.setReducerClass(red.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, map.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, map1.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)? 0 : -1);
return 0;
		
	}

}
