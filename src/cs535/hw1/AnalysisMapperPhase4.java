package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AnalysisMapperPhase4 extends
		Mapper<LongWritable, Text, FloatWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String valueRead = value.toString();
		// try
		// {
		if (valueRead.startsWith(Constants.titleToIterate))
			valueRead = valueRead.substring(Constants.titleToIterate.length());
		if (!valueRead.isEmpty()) {
			String[] firstSplit = valueRead.split(Constants.titleToRank);
			Float pageRankIdealize = Float.parseFloat(firstSplit[1]);
			Float pageRankTaxation = Float.parseFloat(firstSplit[2]);

			String[] thirdSplit = firstSplit[0].split(Constants.linkLink);
			String title = thirdSplit[0].trim();

			context.write(new FloatWritable(pageRankIdealize), new Text(
					Constants.idealized + title));
			context.write(new FloatWritable(pageRankTaxation), new Text(
					Constants.taxation + title));
			
			context.write(new FloatWritable((pageRankTaxation - pageRankIdealize)), new Text(
					Constants.difference + title));

		}

	}

}
