package com.revature.map;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

// import org.apache.hadoop.mapreduce.Mapper;


// Mapper<Key(Input), Value(Input), Key(Output), Value(Output)>
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		// Use split() to turn line into a list, and then use enhanced for loop to iterate over it
		for(String word: line.split("\\W+")) {
			if(word.length() > 0) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

}
