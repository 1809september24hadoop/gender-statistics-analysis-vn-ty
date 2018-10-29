package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FertilityEachDecadeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String col = "SP.DYN.TFRT.IN";
		float info;
		int year = 60;
		if(row[4].contains(col)) {
			for(int i = 5; i < row.length; i++) {
				try {
					info = Float.parseFloat(row[i].replace("\"", ""));
					context.write(new Text(row[0] + "," +  Integer.toString(year++).substring(0,1) + 0), new FloatWritable(info));
				} catch (NumberFormatException e) {}
			}
		}		
	}
}
