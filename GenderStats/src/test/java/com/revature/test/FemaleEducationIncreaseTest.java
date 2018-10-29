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

import com.revature.map.FemaleEducationIncreaseMapper;
import com.revature.reduce.FemaleEducationIncreaseReducer;

public class FemaleEducationIncreaseTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;
	private String row = 
			   "\"Cyprus\",\"CYP\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"23.5976\",\"24.3517\",\"24.9176\",\"25.2156\",\"25.9567\",\"\",\"\",";
	
	@Before
	public void setUp() {
		
		FemaleEducationIncreaseMapper mapper = new FemaleEducationIncreaseMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		FemaleEducationIncreaseReducer reducer = new FemaleEducationIncreaseReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text(row));
		mapDriver.withOutput(new Text("\"Cyprus\""), new FloatWritable((float)24.3517-(float)23.5976));
		mapDriver.withOutput(new Text("\"Cyprus\""), new FloatWritable((float)24.9176-(float)24.3517));
		mapDriver.withOutput(new Text("\"Cyprus\""), new FloatWritable((float)25.2156-(float)24.9176));
		mapDriver.withOutput(new Text("\"Cyprus\""), new FloatWritable((float)25.9567-(float)25.2156));
		mapDriver.runTest();		
	}
	
	@Test
	public void testReducer() {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable((float)24.3517-(float)23.5976));
		values.add(new FloatWritable((float)24.9176-(float)24.3517));
		values.add(new FloatWritable((float)25.2156-(float)24.9176));
		values.add(new FloatWritable((float)25.9567-(float)25.2156));
		reduceDriver.withInput(new Text("Cyprus"), values);
		reduceDriver.withOutput(new Text("Cyprus -- "), new FloatWritable((values.get(0).get()+values.get(1).get()+values.get(2).get()+values.get(3).get())/4));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(row));
		mapReduceDriver.addOutput(new Text("Cyprus -- "), new FloatWritable((
				((float)24.3517-(float)23.5976)+
				((float)24.9176-(float)24.3517)+
				((float)25.2156-(float)24.9176)+
				((float)25.9567-(float)25.2156))/4));
		mapReduceDriver.runTest();		
	}
}
