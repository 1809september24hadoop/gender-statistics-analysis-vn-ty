package com.revature.map;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleEducationIncreaseMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String col = "SE.TER.CUAT.BA.FE.ZS";
		float currentInfo, nextInfo;
		if(row[6].contains(col)) {
			for(int i = 47; i < row.length-1; i++) {
				try {
					currentInfo = Float.parseFloat(row[i].replace("\"", ""));
					nextInfo = Float.parseFloat(row[i+1].replace("\"", ""));
					context.write(new Text(row[0]), new FloatWritable(nextInfo - currentInfo));
				} 
				catch (NumberFormatException e) {}
				catch (IndexOutOfBoundsException e) {}
			}
		}		
	}
}