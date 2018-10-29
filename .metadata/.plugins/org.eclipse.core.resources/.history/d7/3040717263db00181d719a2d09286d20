package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentPercentDifferenceMaleMapper;
import com.revature.reduce.EmploymentPercentDifferenceMaleReducer;

public class EmploymentPercentDifferenceMale {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("you're using this command incorrectly");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(EmploymentPercentDifferenceMale.class);
		job.setJobName("The % change in male employment since 2000.");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(EmploymentPercentDifferenceMaleMapper.class);
		job.setReducerClass(EmploymentPercentDifferenceMaleReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
