package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AnalysisMapperPhase3 extends Mapper<LongWritable, Text, Text, Text> {
//	public static int printData= 1;

//    public Text pageID = new Text("1");
//    Map<String, Integer> str = new HashMap<>();

//    n*n X n*1
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        URI cacheFiles = context.getCacheFiles()[0];
////        for (URI cacheFile : cacheFiles) {
//        File file = new File("Temp");
//        BufferedReader fRead = new BufferedReader(new FileReader(file));
//        String line = "";
//        while ((line = fRead.readLine()) != null) {
//            String[] split = line.split(",");
//            str.put(split[0], Integer.parseInt(split[1]));
//
//        }
////        }
//    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String valueRead = value.toString();
//        try
//        {
        if(valueRead.startsWith(Constants.titleToIterate)) valueRead = valueRead.substring(Constants.titleToIterate.length());
        if (!valueRead.isEmpty()) {
            String[] firstSplit = valueRead.split(Constants.titleToRank);
            Float pageRankIdealize = Float.parseFloat(firstSplit[1]);
            Float pageRankTaxation = Float.parseFloat(firstSplit[2]);
            
            String[] secondSplit = firstSplit[0].split(Constants.linkToOccurance);
            ////////////////////Change By2 While Merging
            int numberOfLinks = Integer.parseInt(secondSplit[1]);
            
            String[] thirdSplit = secondSplit[0].split(Constants.linkLink);
            String title = thirdSplit[0].trim();
            
            
//            Logger logger = Logger.getLogger(MatrixMulMapper.class);
//            for (int i = 0; i < split.length; i++) {
//                String split1 = split[i];
//                System.err.println("-split-" + split1);
//            }

//            Float sum = 0f;
            for (int i = 1; i <= numberOfLinks; i++) {
                context.write(new Text(thirdSplit[i]), new Text(numberOfLinks + Constants.valueSeparator + pageRankIdealize + Constants.valueSeparator + pageRankTaxation));
            }
//            logger.info("");
            context.write(new Text(title), new Text(Constants.titleToIterate + firstSplit[0] + Constants.titleToRank) );
        }
//        }
//        catch(Exception e)
//        {
//        	context.write(new Text("ErrorDetected For String: " + valueRead), new Text("Error: " + e.getMessage()));
//        }
//        context.write(new Text(key.toString()), value);
    }
}
