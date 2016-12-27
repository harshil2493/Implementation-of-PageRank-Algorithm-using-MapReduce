package cs535.hw1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionPhase5 extends Partitioner<Text, FloatWritable> {


	@Override
	public int getPartition(Text key, FloatWritable value, int numberOfReducers) {
		// TODO Auto-generated method stub

		if (key.toString().startsWith(Constants.idealized))
			return 0;
		else if(key.toString().startsWith(Constants.taxation))
			return 1;
		else
			return 2;
		// return characterList.indexOf(text.toString().charAt(0));
	}
}