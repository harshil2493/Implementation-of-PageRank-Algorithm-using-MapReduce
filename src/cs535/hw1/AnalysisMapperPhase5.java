package cs535.hw1;


import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AnalysisMapperPhase5 extends Mapper<LongWritable, Text, Text, FloatWritable>   
{

	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{

//	
		try
		{
		
			String valueRead = value.toString();
			if(!valueRead.contains(Constants.difference))
			{
//				
//				context.write(value, new FloatWritable(0));
//				
			}
			else
			{
//				
//				
				float diff = Float.parseFloat(valueRead.split("\t")[1]);
				context.write(new Text(Constants.difference), new FloatWritable(diff));				
//				
			}
			
			
//			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
	
}
