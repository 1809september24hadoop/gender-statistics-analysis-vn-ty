package com.revature.reduce;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FertilityEachDecadeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		String mill = "19";
		String country = key.toString().split(",")[0];
		String year = key.toString().split(",")[1];		
		for(FloatWritable value: values) {
			sum += value.get();
			count++;
		}
		if(key.toString().contains("10") || key.toString().contains("00")) {mill="20";}
		context.write(new Text(country.replace("\"", "") + " -- " + mill+year), new FloatWritable(sum/count));	
	}
}
