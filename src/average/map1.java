package average;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class map1 extends Mapper<LongWritable,Text,Text,Text>{
	private Text key1 = new Text();
	private Text value = new Text();	
public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException {
	String[] parts = line.toString().split(","); 
	if(parts.length==5)
	{
		key1.set(parts[3]);
		String s=parts[1]+" "+parts[4];
		value.set(s);
	        context.write(key1,value);
	}
} 
}
