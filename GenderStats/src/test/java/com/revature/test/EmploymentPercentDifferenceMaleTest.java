package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.EmploymentPercentDifferenceMaleMapper;
import com.revature.reduce.EmploymentPercentDifferenceMaleReducer;

public class EmploymentPercentDifferenceMaleTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;
	private String row = "\"Australia\",\"AUS\",\"Employers, male (% of male employment)\",\"SL.EMP.MPYR.MA.ZS\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"8.43000030517578\",\"8.60000038146973\",\"8.10999965667725\",\"8.60000038146973\",\"8.55000019073486\",\"\",\"\",";
	
	@Before
	public void setUp() {
		EmploymentPercentDifferenceMaleMapper mapper = new EmploymentPercentDifferenceMaleMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		EmploymentPercentDifferenceMaleReducer reducer = new EmploymentPercentDifferenceMaleReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(row));
		mapDriver.withOutput(new Text("\"Australia\""), new FloatWritable((float)-8.43000030517578));
		mapDriver.withOutput(new Text("\"Australia\""), new FloatWritable((float)8.55000019073486));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float)-8.43000030517578));
		values.add(new FloatWritable((float)8.55000019073486));
		reduceDriver.withInput(new Text("Australia"), values);
		reduceDriver.withOutput(new Text("Australia -- "), new FloatWritable(values.get(1).get()-Math.abs(values.get(0).get())));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(row));
		mapReduceDriver.addOutput(new Text("Australia -- "), new FloatWritable((float)8.55000019073486-(float)8.43000030517578));
		mapReduceDriver.runTest();
	}
}
