package com.revature.reduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class RecentFemaleGraduateReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		for(FloatWritable value: values) {
			sum += value.get();
			count++;
		}
		if((sum/count) > 30) {context.write(new Text(key.toString().replace("\"", "") + " -- "), new FloatWritable(sum/count));}
	}
}