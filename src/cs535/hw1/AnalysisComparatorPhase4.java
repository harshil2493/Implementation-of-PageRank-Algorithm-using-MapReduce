package cs535.hw1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AnalysisComparatorPhase4 extends WritableComparator {

	//Constructor.
	 
	protected AnalysisComparatorPhase4() {
		super(FloatWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FloatWritable k1 = (FloatWritable)w1;
		FloatWritable k2 = (FloatWritable)w2;
		
		return -1 * k1.compareTo(k2);
	}
}