package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisCombinerPhase2 extends Reducer<Text, Text, Text, Text> {

//	public NullWritable nullWritable = NullWritable.get();

	// public String toWriteDown = "";
	// public int count = 0;
	@Override
	protected void reduce(Text text, Iterable<Text> textTypes, Context context)
			throws IOException, InterruptedException {
		// // int counter = 0;
		// String title = text.toString();
		// // String toWriteDown = "";
		// // int count = 0;
		// // ArrayList<String> toWriteDown = new ArrayList<String>();
		// for(Text textToWrite : textTypes)
		// {
		// String gotProtocol = textToWrite.toString();
		// if(!gotProtocol.equals("isLink"))
		// {
		// context.write(new Text("L#" + title + "###" + gotProtocol),
		// nullWritable);
		// // toWriteDown += "#" + gotProtocol;
		// // count++;
		// // toWriteDown.add(gotProtocol);
		// // toWrite += gotProtocol + "##";
		// }
		// }
		// // int sizeOfLinks = toWriteDown.size();
		// // for(String write : toWriteDown)
		// // {
		// // if(count != 0 )
		// // {
		// // context.write(new Text("L#" + title + "##" +toWriteDown + "###"),
		// nullWritable);
		// // }
		// // toWriteDown = "";
		// // count = 0;
		// // }
		// // toWriteDown.clear();
		//
		// context.write(new Text("T#" + title + "###1.0"), nullWritable);
		StringBuffer buffer = new StringBuffer();
//		String toWrite = "";
//		int count = 0;
		// int counter = text.toString();
		for (Text textToWrite : textTypes) {
			// String gotProtocol = textToWrite.toString();
			// if(!gotProtocol.equals("isLink") && !gotProtocol.equals(""))
			// {
			buffer.append(textToWrite.toString());

//			count++;

			// counter++;
			// toWrite += "##" + gotProtocol;
			// }
		}
		context.write(text, new Text(buffer.toString()));

	}
}
