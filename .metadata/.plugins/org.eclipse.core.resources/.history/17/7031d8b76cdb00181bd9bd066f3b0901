package com.revature.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FertilityEachDecadeMapper;
import com.revature.reduce.FertilityEachDecadeReducer;

public class FertilityEachDecadeTest {
	
	private MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
	private ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;
	private String row = "\"El Salvador\",\"SLV\",\"Fertility rate, total (births per woman)\",\"SP.DYN.TFRT.IN\""
			+ ",\"6.67\",\"\",\"6.67\",\"\",\"6.41\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"5.21\",\"\",\"5.31\",\"\",\"5.05\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"4.51\",\"\",\"4.12\",\"\",\"4.20\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"3.78\",\"\",\"3.49\",\"\",\"3.17\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"2.51\",\"\",\"2.36\",\"\",\"2.49\",\"\",\"\",\"\",\"\",\"\""
			+ ",\"2.13\",\"\",\"1.95\",\"\",\"1.75\",\"\",\"\",";
	
	@Before
	public void setUp() {
		FertilityEachDecadeMapper mapper = new FertilityEachDecadeMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		FertilityEachDecadeReducer reducer = new FertilityEachDecadeReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		String vals = 
				   "\"6.67\",\"\",\"6.67\",\"\",\"6.41\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"5.21\",\"\",\"5.31\",\"\",\"5.05\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"4.51\",\"\",\"4.12\",\"\",\"4.20\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"3.78\",\"\",\"3.49\",\"\",\"3.17\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"2.51\",\"\",\"2.36\",\"\",\"2.49\",\"\",\"\",\"\",\"\",\"\""
				+ ",\"2.13\",\"\",\"1.95\",\"\",\"1.75\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(vals));
		mapDriver.withOutput(new Text("\"El Salvador\",60"), new FloatWritable((float)6.67));
		mapDriver.withOutput(new Text("\"El Salvador\",62"), new FloatWritable((float)6.67));
		mapDriver.withOutput(new Text("\"El Salvador\",64"), new FloatWritable((float)6.41));
		mapDriver.withOutput(new Text("\"El Salvador\",70"), new FloatWritable((float)5.21));
		mapDriver.withOutput(new Text("\"El Salvador\",72"), new FloatWritable((float)5.31));
		mapDriver.withOutput(new Text("\"El Salvador\",74"), new FloatWritable((float)5.05));
		mapDriver.withOutput(new Text("\"El Salvador\",80"), new FloatWritable((float)4.51));
		mapDriver.withOutput(new Text("\"El Salvador\",82"), new FloatWritable((float)4.12));
		mapDriver.withOutput(new Text("\"El Salvador\",84"), new FloatWritable((float)4.20));
		mapDriver.withOutput(new Text("\"El Salvador\",90"), new FloatWritable((float)3.78));
		mapDriver.withOutput(new Text("\"El Salvador\",92"), new FloatWritable((float)3.49));
		mapDriver.withOutput(new Text("\"El Salvador\",94"), new FloatWritable((float)3.17));
		mapDriver.withOutput(new Text("\"El Salvador\",00"), new FloatWritable((float)2.51));
		mapDriver.withOutput(new Text("\"El Salvador\",02"), new FloatWritable((float)2.36));
		mapDriver.withOutput(new Text("\"El Salvador\",04"), new FloatWritable((float)2.49));
		mapDriver.withOutput(new Text("\"El Salvador\",10"), new FloatWritable((float)2.13));
		mapDriver.withOutput(new Text("\"El Salvador\",12"), new FloatWritable((float)1.95));
		mapDriver.withOutput(new Text("\"El Salvador\",14"), new FloatWritable((float)1.75));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		
		values.add(new FloatWritable((float)6.67));
		values.add(new FloatWritable((float)6.67));
		values.add(new FloatWritable((float)6.41));
		values.add(new FloatWritable((float)5.21));
		values.add(new FloatWritable((float)5.31));
		values.add(new FloatWritable((float)5.05));
		values.add(new FloatWritable((float)4.51));
		values.add(new FloatWritable((float)4.12));
		values.add(new FloatWritable((float)4.20));
		values.add(new FloatWritable((float)3.78));
		values.add(new FloatWritable((float)3.49));
		values.add(new FloatWritable((float)3.17));
		values.add(new FloatWritable((float)2.51));
		values.add(new FloatWritable((float)2.36));
		values.add(new FloatWritable((float)2.49));
		values.add(new FloatWritable((float)2.13));
		values.add(new FloatWritable((float)1.95));
		values.add(new FloatWritable((float)1.75));
		reduceDriver.withInput(new Text("El Salvador,60"), values.subList(0, 2));
		reduceDriver.withInput(new Text("El Salvador,70"), values.subList(2, 4));
		reduceDriver.withInput(new Text("El Salvador,80"), values.subList(4, 6));
		reduceDriver.withInput(new Text("El Salvador,90"), values.subList(6, 8));
		reduceDriver.withInput(new Text("El Salvador,00"), values.subList(8, 10));
		reduceDriver.withInput(new Text("El Salvador,10"), values.subList(10, 12));
		reduceDriver.withOutput(new Text("El Salvador -- 2010"), new FloatWritable((float)3.33));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text(row));
		mapReduceDriver.addOutput(new Text("El Salvador -- 1960"), new FloatWritable(((float)6.67+(float)6.67+(float)6.41)/3));
		mapReduceDriver.addOutput(new Text("El Salvador -- 1970"), new FloatWritable(((float)5.21+(float)5.31+(float)5.05)/3));
		mapReduceDriver.addOutput(new Text("El Salvador -- 1980"), new FloatWritable(((float)4.51+(float)4.12+(float)4.20)/3));
		mapReduceDriver.addOutput(new Text("El Salvador -- 1990"), new FloatWritable(((float)3.78+(float)3.49+(float)3.17)/3));
		mapReduceDriver.addOutput(new Text("El Salvador -- 2000"), new FloatWritable(((float)2.51+(float)2.36+(float)2.49)/3));
		mapReduceDriver.addOutput(new Text("El Salvador -- 2010"), new FloatWritable(((float)2.13+(float)1.95+(float)1.75)/3));
		mapReduceDriver.runTest();
	}

}
