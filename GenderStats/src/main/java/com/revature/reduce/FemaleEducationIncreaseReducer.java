package com.revature.reduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleEducationIncreaseReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
			throws IOException, InterruptedException {
		int count = 0;
		float increase = 0;
		for(FloatWritable value : values) {
			increase += value.get();
			//increase = 4;
			count++;
		}
		context.write(new Text(key.toString().replace("\"", "") + " -- "), new FloatWritable(increase/count));
	}
}