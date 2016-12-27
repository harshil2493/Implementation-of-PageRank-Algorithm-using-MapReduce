package cs535.hw1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionPhase4 extends Partitioner<FloatWritable, Text> {


	@Override
	public int getPartition(FloatWritable key, Text value, int numberOfReducers) {
		// TODO Auto-generated method stub

		if (value.toString().startsWith(Constants.idealized))
			return 0;
		else if(value.toString().startsWith(Constants.taxation))
			return 1;
		else
			return 2;
		// return characterList.indexOf(text.toString().charAt(0));
	}
}