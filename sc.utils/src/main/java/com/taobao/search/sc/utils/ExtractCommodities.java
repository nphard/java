package com.taobao.search.sc.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class ExtractCommodities extends Configured implements Tool {
		
	public static class MapperClass extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		
		private Text value = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter ) throws IOException {
			String line = value.toString();
			String[] fields = line.toString().split("\u0001");
			long comid = Long.parseLong(fields[0]);
			value.set(fields[60]+"/" + fields[61] + "/" + fields[62] + fields[63] + fields[64]);
			output.collect(new LongWritable(comid), value);
		}
	}
		
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(),ExtractCommodities.class);
		conf.setJobName("ExtractCommodities");
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(MapperClass.class);
		conf.setCombinerClass(ReducerClass.clas);
		conf.setReducerClass(ReducerClass.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		int ret = ToolRunner.run(new Configuration(),new ExtractCommodities(), args);
		System.exit(ret);
	}

}
