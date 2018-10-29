package com.revature.map;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class EmploymentPercentDifferenceFemaleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		float infoOldest, infoMostRecent = 0;
		boolean firstFound = false;
		String[] row = value.toString().split(",");
		if(row[4].contains("SL.EMP.MPYR.FE.ZS")) {
			for(int i = 45; i < row.length; i++) {
				if(!firstFound) {
					try {
						infoOldest = 0 - Float.parseFloat(row[i].replace("\"", ""));
						firstFound = true;
						context.write(new Text(row[0]), new FloatWritable(infoOldest));
					} catch (NumberFormatException e) {}
				} else {
					try {
						infoMostRecent = Float.parseFloat(row[i].replace("\"", ""));
					} catch (NumberFormatException e) {}
				}
			}
			context.write(new Text(row[0]), new FloatWritable(infoMostRecent));
		}		
	}
}
