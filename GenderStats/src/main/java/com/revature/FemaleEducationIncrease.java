package com.revature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.FloatWritable;
import com.revature.map.FemaleEducationIncreaseMapper;
import com.revature.reduce.FemaleEducationIncreaseReducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FemaleEducationIncrease {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("you're not using this command properly");
			System.exit(-1);
		}
		Job job = new Job();
		job.setJarByClass(FemaleEducationIncrease.class);
		job.setJobName("Countries with less than 30% female graduates!");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FemaleEducationIncreaseMapper.class);
		job.setReducerClass(FemaleEducationIncreaseReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
