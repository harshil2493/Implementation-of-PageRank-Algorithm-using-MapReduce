package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;

//import cs535.testing.CountMe;

public class AnalysisReducerPhase5  extends Reducer<Text, FloatWritable, Text, NullWritable> {

	public NullWritable nullWritable = NullWritable.get();
	long toDivide;
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> floatTypes, Context context)
			throws IOException, InterruptedException {
		
		
		
		
		
//		if(!key.toString().startsWith(Constants.difference))
//		{
//			context.write(key, nullWritable);
//		}
//		
//		else
//		{
			toDivide = Long.parseLong(context.getConfiguration().get("countOfLinks"));
			float answer = 0;
			
			for(FloatWritable f : floatTypes)
			{
				
				answer += f.get();
					
				
			}
			double average = answer / toDivide;
			context.write(new Text("Average: " + average), nullWritable);
//		}
		
		
		
	}
	

}
