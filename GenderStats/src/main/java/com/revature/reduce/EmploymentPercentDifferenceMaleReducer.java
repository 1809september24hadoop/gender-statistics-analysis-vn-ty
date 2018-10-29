package com.revature.reduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EmploymentPercentDifferenceMaleReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException {
		float infoOldest = 0, infoMostRecent = 0;
		float count =  0;
		for(FloatWritable val: values) {
			count++;
			if(val.get() < 0) {
				infoOldest = Math.abs(val.get());				
			} else {
				infoMostRecent = val.get();
			}
		}
		if(count>1) {context.write(new Text(key.toString().replace("\"", "") + " -- "), new FloatWritable(infoMostRecent - infoOldest));}
	}
}