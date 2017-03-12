package average;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class red extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
		int max = 0;
		int max1 =0;
		String sal="";
		HashMap<String,String> det= new HashMap<String,String>();
		for(Text val:values){
			String[] parts = val.toString().split(" ");
			det.put(parts[1],parts[0]);
			max=Integer.parseInt(parts[1]);
			if(max>max1)
				{max1=max;}
	}
		
		sal=det.get((String)(max1+"").toString());
		String s=(String)(max1+" "+sal).toString();
		if(det.size()!=1)
	context.write(key, new Text(s));
	
	}
}
