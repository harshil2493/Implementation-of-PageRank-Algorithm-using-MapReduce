package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalysisReducerPhase3 extends Reducer<Text, Text, Text, NullWritable> {

	public static final float beta = (float) 0.85;
	public static final float oneMinusBeta = (float) (1 - 0.85);
	public NullWritable n = NullWritable.get();
	// DecimalFormat df = new DecimalFormat("#.##");
	public String stringGot = "";

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// Configuration conf = context.getConfiguration();
		// String[] value;
		// HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
		// HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
		// for (Text val : values) {
		// value = val.toString().split(",");
		// if (value[0].equals("A")) {
		// hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
		// } else {
		// hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
		// }
		// }
		// int n = Integer.parseInt(conf.get("sameD"));
		// int n = 3;
		// float result = 0.0f;
		// float a_ij;
		// float b_jk;
		// for (int j = 0; j < n; j++) {
		// a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
		// b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
		// result += a_ij * b_jk;
		// }
		// if (result != 0.0f) {
		// context.write(null, new Text(key.toString() + "," +
		// Float.toString(result)));
		// }
		// String str = "";
		// for (Text value : values) {
		// str += value.toString();
		// }
//		if(!key.toString().startsWith("ErrorDetected For String"))
//		{
		StringBuffer toWrite = new StringBuffer();
//		int length = 0;
		double pageRankIdealize = 0.0;
		double pageRankTaxed = 0.0;
		int totalTitles = 5000000;
		boolean gotData = false;
		for (Text value : values) {
			stringGot = value.toString();
			if (!stringGot.startsWith(Constants.titleToIterate)) {
				// stringGot = (value.toString());
				String[] split = stringGot.split(Constants.valueSeparator);
				int share = Integer.parseInt(split[0]);
				// totalTitles = Integer.parseInt(split[2]);
				double previousPageRankIdealize = Float.parseFloat(split[1]);
				pageRankIdealize += previousPageRankIdealize / share;
				double previousPageRankTaxed = Float.parseFloat(split[2]);
				pageRankTaxed += (previousPageRankTaxed) / share;
			} else {
//				context.write(new Text("Title Recieved: " + stringGot + ""), n);
				toWrite.append(stringGot);
				gotData = true;
			}

		}
		// }
		// }

		// float taxedValue = beta * pageRankIdealize + oneMinusBeta
		// toWrite = new StringBuffer(toWrite.substring(0,
		// toWrite.lastIndexOf("^") + 1));

		 if (!gotData) {
		 // Dead End Logic
		 toWrite.append(Constants.titleToIterate + key.toString() + Constants.linkLink + "No_Link" + Constants.linkToOccurance + "0" + Constants.titleToRank);
		
		 }
		//
		pageRankTaxed = ((pageRankTaxed * beta) + (oneMinusBeta));
		// toWrite.append();
		toWrite.append((pageRankIdealize) + Constants.titleToRank + (pageRankTaxed));
		// context.write((key), new Text(pageRankIdealize + "\t" +
		// pageRankTaxed));
		// str = str.substring(0, str.length() - 1);
		context.write(new Text(toWrite.toString()), n);
		// String str = "";
		// for (Text value : values) {
		// str += value.toString();
		// }
		// context.write(key, new Text(str));
//		}
//		else
//		{
//			for(Text v : values)
//				context.write(new Text(key.toString() + v.toString()), n);
//		}
	}
}
