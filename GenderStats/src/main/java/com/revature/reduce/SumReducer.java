package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int wordCount = 0;
		//int maxCount = 0;
		// Text maxKey = null;
		for(IntWritable value: values) {
			wordCount += value.get();
			//if(wordCount > maxCount) {
				//maxCount = wordCount;
			//}
		}
		// context.write(maxKey,  new IntWritable(maxCount));
		context.write(key, new IntWritable(wordCount));
	}
}
