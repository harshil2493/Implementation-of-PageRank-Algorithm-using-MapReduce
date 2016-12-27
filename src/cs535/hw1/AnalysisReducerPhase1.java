package cs535.hw1;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AnalysisReducerPhase1 extends Reducer<Text, Text, Text, NullWritable>{
	NullWritable nullWritable = NullWritable.get();
	int redirectLength = Constants.phase1RedirectConstant.length();
	@Override
	protected void reduce(Text text, Iterable<Text> textTypes, Context context) throws IOException, InterruptedException 
	{
		Set<String> uniqueLinks = new HashSet<String>();
		String originalTitle = "";
		for (Text textToWrite : textTypes) {
			
			String dataString = textToWrite.toString();
			if (dataString.contains(Constants.phase1TitleConstant)) {
				originalTitle = text.toString();
			} else if (dataString.contains(Constants.phase1RedirectConstant)) {
				
				dataString = dataString.substring(redirectLength);
				// uniqueLinks.add(dataString);
				originalTitle = dataString;
			}
			// originalTitle = dataString;
			// }
			else {
				if(dataString.length() != 0)
					uniqueLinks.add(dataString);
			}

		}
		// float numberOfLinks = uniqueLinks.size();
		// float numberAssigned = (float) (1.0 / numberOfLinks);
		if (!originalTitle.equals("") ) {
			
			for (String uniqueLink : uniqueLinks) {

				context.write(new Text(uniqueLink + Constants.titleLink + originalTitle), NullWritable.get());
			}
		}
	}
}
