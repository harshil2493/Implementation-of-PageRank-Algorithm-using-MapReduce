package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerPhaseLinkCount extends Reducer<Text, NullWritable, Text, NullWritable> {

	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		
//		Counter c =(Counter) context.getCounter(CountMe.countOfLinks);
		
		
		context.getCounter(CountMe.countOfLinks).increment(1L);;//		context.
		
	
//		CountMe.Good.
	}
}
