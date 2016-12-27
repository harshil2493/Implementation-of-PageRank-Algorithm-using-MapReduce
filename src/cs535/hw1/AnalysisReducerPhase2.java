package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerPhase2 extends Reducer<Text, Text, Text, Text> {

	public NullWritable nullWritable = NullWritable.get();
	
	@Override
	protected void reduce(Text text, Iterable<Text> textTypes, Context context)
			throws IOException, InterruptedException {
//		int counter = 1;
//		if (text.toString().equals("")) {
//			for (Text textLinks : textTypes) {
//				context.write(new Text(String.valueOf(counter)), textLinks);
//				counter++;
//			}
//		} else {
//			String toWrite = "#####";
//			for (Text textToWrite : textTypes) {
//				toWrite += textToWrite.toString() + "#####";
//				counter++;
//				// context.write(text, textToWrite);
//
//			}
//			context.write(text, new Text(String.valueOf(counter)+toWrite));
//		}
		
//		String toWrite = "##";
//		int counter = 0;
//		String title = text.toString();
		
		StringBuffer toWrite = new StringBuffer();
//		int counter = 0;
//		int counter = text.toString();
		for(Text textToWrite : textTypes)
		{
//			String gotProtocol = textToWrite.toString();
//			if(!gotProtocol.equals("isLink") && !gotProtocol.equals(""))
//			{	
//			String[] write = textToWrite.toString().split("##COUNT##");
//				toWrite += write[0];
//				counter += Integer.parseInt(write[1]);
			
//			context.write(text, textToWrite);
				String toW = textToWrite.toString();
				if(!toW.isEmpty())
					toWrite.append(toW); 
				
				
//				counter++;
//				toWrite += "##" + gotProtocol;
//			}
		}
		int occur = (toWrite.length() - toWrite.toString().replace(Constants.linkLink, "").length()) / 3;
		if(toWrite.length() !=0)
			context.write(text, new Text(toWrite.toString() + Constants.linkToOccurance + occur + Constants.titleToRank + "1.0" + Constants.titleToRank + "1.0"));

		
		
//		for(Text textToWrite : textTypes)
//		{
////			String gotProtocol = textToWrite.toString();
////			if(!gotProtocol.equals("isLink") && !gotProtocol.equals(""))
////			{
//				context.write(new Text("L#" + text.toString()), new Text("###"+textToWrite.toString()));
//
////				counter++;
////				toWrite += "##" + gotProtocol;
////			}
//		}
//		context.write(new Text("T#" + title + "##1.0"), nullWritable);
		// for(String uniqueLink : uniqueLinks)
		// {
		//
		// }
	}
}
