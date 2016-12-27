package cs535.hw1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import cs535.testing.CountMe;

public class AnalysisJob {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		// //
		// // // Phase 1 --- Getting Title To Links...
		Job job = Job.getInstance(conf, "Wiki_hkshah");
		// // //
		// // //
		// // //
		job.setJarByClass(AnalysisJob.class);
		// // //
		job.setInputFormatClass(XmlInputFormat.class);
		// // //
		job.setMapperClass(AnalysisMapperPhase1.class);
		// // // //
		// // // // job.setPartitionerClass(AnalysisPartition.class);
		// // // //
		job.setReducerClass(AnalysisReducerPhase1.class);
		// // //
		job.setNumReduceTasks(40);
		// // //
		// // //
		// // //
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileSystem fileSystem = FileSystem.get(conf);
		//
		String outputPath = args[1] + "_Phase1";
		// // //
		if (fileSystem.exists(new Path(outputPath)))
			fileSystem.delete(new Path(outputPath), true);
		//
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		//
		// //
		// //
		// //
		// // //Phase 2 ----- Removing Unwanted Links
		// //
		//
		Job jobPhase2 = Job.getInstance(conf, "Wiki_hkshah");
		// //
		// //
		// //
		jobPhase2.setJarByClass(AnalysisJob.class);
		// //
		// //
		jobPhase2.setMapperClass(AnalysisMapperPhase2.class);
		jobPhase2.setCombinerClass(AnalysisCombinerPhase2.class);
		// // ////// job.setPartitionerClass(AnalysisPartition.class);
		// // // //////
		jobPhase2.setReducerClass(AnalysisReducerPhase2.class);
		// //
		jobPhase2.setNumReduceTasks(40);
		// //
		// //
		// String outputPath = args[0];
		jobPhase2.setMapOutputKeyClass(Text.class);
		jobPhase2.setMapOutputValueClass(Text.class);
		// //
		jobPhase2.setOutputKeyClass(Text.class);
		jobPhase2.setOutputValueClass(NullWritable.class);
		// // FileSystem fileSystem = FileSystem.get(conf);
		FileInputFormat.addInputPath(jobPhase2, new Path(outputPath));
		// // //
		// // //
		String outputPathPhase2 = args[1] + "_Phase2";
		// //
		if (fileSystem.exists(new Path(outputPathPhase2)))
			fileSystem.delete(new Path(outputPathPhase2), true);
		// //
		FileOutputFormat.setOutputPath(jobPhase2, new Path(outputPathPhase2));
		jobPhase2.waitForCompletion(true);

		fileSystem.delete(new Path(outputPath), true);

		//
		//
		//

		String inputToCount = outputPathPhase2;
		// String inputToCount = args[0];

		Job jobToCount = Job.getInstance(conf, "Wiki_hkshah");
		// //
		// //
		// //
		jobToCount.setJarByClass(AnalysisJob.class);
		// //
		// job.setInputFormatClass(XmlInputFormat.class);
		// //
		jobToCount.setMapperClass(AnalysisMapperPhaseLinkCount.class);
		// // //
		// // // job.setPartitionerClass(AnalysisPartition.class);
		// // //
		jobToCount.setReducerClass(AnalysisReducerPhaseLinkCount.class);
		// //
		jobToCount.setNumReduceTasks(40);
		// //
		// //
		// //
		jobToCount.setMapOutputKeyClass(Text.class);
		jobToCount.setMapOutputValueClass(NullWritable.class);
		//
		jobToCount.setOutputKeyClass(Text.class);
		jobToCount.setOutputValueClass(NullWritable.class);
		//
		FileInputFormat.addInputPath(jobToCount, new Path(inputToCount));
		// FileSystem fileSystem = FileSystem.get(conf);

		String outputPathToCount = args[1] + "_ToCount";
		//
		if (fileSystem.exists(new Path(outputPathToCount)))
			fileSystem.delete(new Path(outputPathToCount), true);

		FileOutputFormat.setOutputPath(jobToCount, new Path(outputPathToCount));
		jobToCount.waitForCompletion(true);

		Counters counters = jobToCount.getCounters();

		// counters.

		// long counter = counters.findCounter("Map-Reduce Framework",
		// "Map output records").getValue();
		System.out.println("(Before Iteration) Total Links Found: "
				+ new Long(counters.findCounter(CountMe.countOfLinks)
						.getValue()));
		fileSystem.delete(new Path(outputPathToCount), true);
		// String inputToCount = outputPathPhase2;

		// String inputPathIteration = args[1] + "_10";
		String inputPathIteration = outputPathPhase2;
		for (int i = 1; i <= 25; i++) {

			Job jobNew = Job.getInstance(conf, "Wiki_hkshah");

			jobNew.setJarByClass(AnalysisJob.class);

			// job.setInputFormatClass(TextInputFormat.class);
			jobNew.setMapperClass(AnalysisMapperPhase3.class);
			// jobNew.setPartitionerClass(AnalysisPartitionPhase3.class);
			//
			// job.setPartitionerClass(AnalysisPartition.class);
			//
			jobNew.setReducerClass(AnalysisReducerPhase3.class);

			jobNew.setNumReduceTasks(40);
			jobNew.setMapOutputKeyClass(Text.class);
			jobNew.setMapOutputValueClass(Text.class);

			jobNew.setOutputKeyClass(Text.class);
			jobNew.setOutputValueClass(NullWritable.class);
			String outputPathIteration = args[1] + "_" + i;
			// job.addCacheFile(new URI("hdfs://raleigh:32201" + args[1] +
			// "#Temp"));
			FileInputFormat.addInputPath(jobNew, new Path(inputPathIteration));

			if (fileSystem.exists(new Path(outputPathIteration))) {
				fileSystem.delete(new Path(outputPathIteration), true);
			}

			FileOutputFormat.setOutputPath(jobNew,
					new Path(outputPathIteration));
			jobNew.waitForCompletion(true);

			if (i >= 2) {
				fileSystem.delete(new Path(args[1] + "_" + (i - 1)), true);
			}
			inputPathIteration = outputPathIteration;
		}

		// Counters counter = jobToCount.getCounters();

		// counters.

		// long counter = counters.findCounter("Map-Reduce Framework",
		// "Map output records").getValue();
		System.out.println("(After Iteration) Total Links Found: "
				+ new Long(counters.findCounter(CountMe.countOfLinks)
						.getValue()));

		// Descending
		Configuration confNew = new Configuration();
		confNew.set("countOfLinks", String.valueOf((counters
				.findCounter(CountMe.countOfLinks).getValue())));

		Job jobPhase4 = Job.getInstance(confNew, "Wiki_hkshah");

		// Counters countersNew = jobPhase4.getCounters();

		// counters.

		// long counter = counters.findCounter("Map-Reduce Framework",
		// "Map output records").getValue();
		// System.out.println("(Before Iteration) Total Links Found: "
		// countersNew.findCounter(CountMe.countOfLinks).setValue(new
		// Long(counters.findCounter(CountMe.countOfLinks).getValue()));

		jobPhase4.setJarByClass(AnalysisJob.class);

		// job.setInputFormatClass(TextInputFormat.class);
		jobPhase4.setMapperClass(AnalysisMapperPhase4.class);

		jobPhase4.setSortComparatorClass(AnalysisComparatorPhase4.class);
		jobPhase4.setPartitionerClass(AnalysisPartitionPhase4.class);
		//
		// job.setPartitionerClass(AnalysisPartition.class);
		//
		jobPhase4.setReducerClass(AnalysisReducerPhase4.class);

		jobPhase4.setNumReduceTasks(3);
		jobPhase4.setMapOutputKeyClass(FloatWritable.class);
		jobPhase4.setMapOutputValueClass(Text.class);

		jobPhase4.setOutputKeyClass(Text.class);
		jobPhase4.setOutputValueClass(FloatWritable.class);
		String outputPathSorted = args[1] + "_Sorted_Ranks";
		// job.addCacheFile(new URI("hdfs://raleigh:32201" + args[1] +
		// "#Temp"));
		FileInputFormat.addInputPath(jobPhase4, new Path(inputPathIteration));

		if (fileSystem.exists(new Path(outputPathSorted))) {
			fileSystem.delete(new Path(outputPathSorted), true);
		}

		FileOutputFormat.setOutputPath(jobPhase4, new Path(outputPathSorted));
		jobPhase4.waitForCompletion(true);
		
		
		/*
		
		
		// Descending
		Configuration confNew = new Configuration();
		confNew.set("countOfLinks", String.valueOf(7107930));

		Job jobPhase4 = Job.getInstance(confNew, "Wiki_hkshah");

		// Counters countersNew = jobPhase4.getCounters();

		// counters.

		// long counter = counters.findCounter("Map-Reduce Framework",
		// "Map output records").getValue();
		// System.out.println("(Before Iteration) Total Links Found: "
		// countersNew.findCounter(CountMe.countOfLinks).setValue(new
		// Long(counters.findCounter(CountMe.countOfLinks).getValue()));

		jobPhase4.setJarByClass(AnalysisJob.class);

		// job.setInputFormatClass(TextInputFormat.class);
		jobPhase4.setMapperClass(AnalysisMapperPhase4.class);

		jobPhase4.setSortComparatorClass(AnalysisComparatorPhase4.class);
		jobPhase4.setPartitionerClass(AnalysisPartitionPhase4.class);
		//
		// job.setPartitionerClass(AnalysisPartition.class);
		//
		jobPhase4.setReducerClass(AnalysisReducerPhase4.class);

		jobPhase4.setNumReduceTasks(3);
		jobPhase4.setMapOutputKeyClass(FloatWritable.class);
		jobPhase4.setMapOutputValueClass(Text.class);

		jobPhase4.setOutputKeyClass(Text.class);
		jobPhase4.setOutputValueClass(FloatWritable.class);
		String outputPathSorted = args[1] + "_Sorted_Ranks";
		
		String inputPathIteration = "/cs535_Wiki_25";
		
		// job.addCacheFile(new URI("hdfs://raleigh:32201" + args[1] +
		// "#Temp"));
		FileInputFormat.addInputPath(jobPhase4, new Path(inputPathIteration));

		FileSystem fileSystem = FileSystem.get(confNew);
		
		if (fileSystem.exists(new Path(outputPathSorted))) {
			fileSystem.delete(new Path(outputPathSorted), true);
		}

		FileOutputFormat.setOutputPath(jobPhase4, new Path(outputPathSorted));
		jobPhase4.waitForCompletion(true);
		
		*/
		
//		Configuration confNew = new Configuration();
		confNew.set("countOfLinks", String.valueOf(7107930));
		
		Job jobPhase5 = Job.getInstance(confNew, "final_ending");

		// Counters countersNew = jobPhase4.getCounters();

		// counters.

		// long counter = counters.findCounter("Map-Reduce Framework",
		// "Map output records").getValue();
		// System.out.println("(Before Iteration) Total Links Found: "
		// countersNew.findCounter(CountMe.countOfLinks).setValue(new
		// Long(counters.findCounter(CountMe.countOfLinks).getValue()));

		jobPhase5.setJarByClass(AnalysisJob.class);

		// job.setInputFormatClass(TextInputFormat.class);
		jobPhase5.setMapperClass(AnalysisMapperPhase5.class);

//		jobPhase5.setSortComparatorClass(AnalysisComparatorPhase4.class);
//		jobPhase5.setPartitionerClass(AnalysisPartitionPhase5.class);
		//
		// job.setPartitionerClass(AnalysisPartition.class);
		//
		jobPhase5.setReducerClass(AnalysisReducerPhase5.class);

//		jobPhase5.setNumReduceTasks(3);
		jobPhase5.setMapOutputKeyClass(Text.class);
		jobPhase5.setMapOutputValueClass(FloatWritable.class);

		jobPhase5.setOutputKeyClass(Text.class);
		jobPhase5.setOutputValueClass(NullWritable.class);
//		String outputPathSorted = args[1] + "_Sorted_Ranks";
//		String outputPathSorted = args[1] + "_Sorted_Ranks";
		// job.addCacheFile(new URI("hdfs://raleigh:32201" + args[1] +
		// "#Temp"));
//		String outputPathSorted = args[1] + "_Sorted_Ranks";
		FileInputFormat.addInputPath(jobPhase5, new Path(outputPathSorted));
		String outputPathFinal = args[1] + "_Sorted_Ranks_With_Average";
		
//		FileSystem fileSystem = FileSystem.get(confNew);
		
		if (fileSystem.exists(new Path(outputPathFinal))) {
			fileSystem.delete(new Path(outputPathFinal), true);
		}

		FileOutputFormat.setOutputPath(jobPhase5, new Path(outputPathFinal));
		jobPhase5.waitForCompletion(true);
//		fileSystem.delete(new Path(outputPathSorted), true);
		
		System.out.println("[INFO] Job Is Done..");
		System.out.println("[INFO] Thank You For Using Hadoop Cluster.");
		// inputPathIteration = outputPathSorted;

		//
		//
		//

		// fileSystem.delete(new Path(outputPath), true);

		// Phase 3
		//
		// Job jobPhase3 = Job.getInstance(conf, "Wiki_hkshah");
		// //
		// //
		// //
		// jobPhase3.setJarByClass(AnalysisJob.class);
		// //
		// //
		// jobPhase3.setMapperClass(AnalysisMapperPhase3.class);
		// ////
		// //// jobPhase3.setPartitionerClass(AnalysisPartitionPhase3.class);
		// ////
		// jobPhase3.setCombinerClass(AnalysisCombinerPhase3.class);
		// jobPhase3.setReducerClass(AnalysisReducerPhase3.class);
		// //
		// jobPhase3.setNumReduceTasks(40);
		// //
		// // jobPhase3.set
		// //
		// jobPhase3.setMapOutputKeyClass(Text .class);
		// jobPhase3.setMapOutputValueClass(Text.class);
		// //
		//
		// jobPhase3.setOutputKeyClass(Text.class);
		// jobPhase3.setOutputValueClass(Text.class);
		// //
		// FileInputFormat.addInputPath(jobPhase3, new Path(outputPathPhase2));
		// //
		// //
		// String outputPathPhase3 = args[1] + "_Phase3";
		// //
		// if (fileSystem.exists(new Path(outputPathPhase3)))
		// fileSystem.delete(new Path(outputPathPhase3), true);
		// //
		// FileOutputFormat.setOutputPath(jobPhase3, new
		// Path(outputPathPhase3));
		// jobPhase3.waitForCompletion(true);

		// fileSystem.delete(new Path(outputPathPhase2), true);

		// Job newJob = Job.getInstance(conf, "US_Census_hkshah_AfterAnalysis");
		//
		// newJob.setJarByClass(AnalysisJob.class);
		//
		// newJob.setMapperClass(AfterAnalysisNewMapper.class);
		//
		// newJob.setReducerClass(AfterAnalysisNewReducer.class);
		//
		// newJob.setNumReduceTasks(1);
		//
		// newJob.setMapOutputKeyClass(Text .class);
		// newJob.setMapOutputValueClass(Text.class);
		//
		// newJob.setOutputKeyClass(NullWritable.class);
		// newJob.setOutputValueClass(Text.class);
		//
		// FileInputFormat.addInputPath(newJob, new Path(outputPath));
		// FileSystem fileSystemNew = FileSystem.get(conf);
		//
		// String outputPathNew = args[1];
		//
		// if (fileSystemNew.exists(new Path(outputPathNew)))
		// fileSystemNew.delete(new Path(outputPathNew), true);
		//
		// FileOutputFormat.setOutputPath(newJob, new Path(outputPathNew));
		// boolean isTaskDone = newJob.waitForCompletion(true);
		//
		// if(isTaskDone)
		// {
		// fileSystemNew.delete(new Path(outputPath), true);
		// System.exit(0);
		// }
		// else
		// {
		// System.exit(1);
		// }
	}
}
