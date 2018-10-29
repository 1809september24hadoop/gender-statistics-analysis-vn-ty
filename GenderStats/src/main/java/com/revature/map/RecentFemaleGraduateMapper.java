package com.revature.map;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class RecentFemaleGraduateMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		if(row[5].contains("SE.PRM.CMPL.FE.ZS")) {
			for(int i = row.length-1; i > 6; i--) {
				float info = 0;
				try {
					info = Float.parseFloat(row[i].replace("\"", ""));
					context.write(new Text(row[0]), new FloatWritable(info));
				} catch (NumberFormatException e) {}
			}
		}		
	}
}