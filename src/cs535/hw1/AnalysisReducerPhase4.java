package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;

//import cs535.testing.CountMe;

public class AnalysisReducerPhase4  extends Reducer<FloatWritable, Text, Text, FloatWritable> {

	public NullWritable nullWritable = NullWritable.get();
	long toDivide;
	@Override
	protected void reduce(FloatWritable floatNumber, Iterable<Text> textTypes, Context context)
			throws IOException, InterruptedException {
		
		
		
		toDivide = Long.parseLong(context.getConfiguration().get("countOfLinks"));
		
		
		for(Text titles : textTypes)
		{
			
			// counters.

			// long counter = counters.findCounter("Map-Reduce Framework",
			// "Map output records").getValue();

				float floatToWrite = floatNumber.get() / toDivide;
				context.write(new Text(titles.toString()), new FloatWritable(floatToWrite));
		}
		
	}
	

}
