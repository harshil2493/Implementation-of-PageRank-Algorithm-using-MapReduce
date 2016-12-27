package cs535.hw1;

import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AnalysisPartitionPhase3 extends Partitioner<Text, Text> {
	Random r = new Random();

	@Override
	public int getPartition(Text key, Text value, int numberOfReducers) {
		// TODO Auto-generated method stub

		if (key.toString().startsWith("ErrorDetected"))
			return 42;
		else
			return (r.nextInt(42));
		// return characterList.indexOf(text.toString().charAt(0));
	}

}