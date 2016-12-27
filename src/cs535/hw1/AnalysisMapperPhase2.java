package cs535.hw1;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AnalysisMapperPhase2 extends Mapper<LongWritable, Text, Text, Text>   
{


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
//		String[] valueRead = value.toString().split("#");
//		String link = valueRead[0].trim();
//		String title = valueRead[1];
//		
////		context.write(new Text(link), new Text("isLink"));	
////		context.write(new Text(title), new Text("isLink"));	
//		context.write(new Text(title), new Text(link));
//	
		try
		{
		
			String[] valueRead = value.toString().split(Constants.titleLink);
			String title = valueRead[0].trim();
			String link = valueRead[1].trim();
			
//			context.write(new Text(link), new Text("isLink"));	
//			context.write(new Text(title), new Text("isLink"));	
			if(!title.isEmpty() && !link.isEmpty())
				context.write(new Text(title), new Text(Constants.linkLink + link));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}
	
}
