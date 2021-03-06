package com.taobao.search.sc.utils;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;

import ws.Wsjni;

public class CollectCatsOfShop extends Configured implements Tool {

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable,Text> {

		static enum Counters {
			Items
		}
		private Text recordValue = new Text();

		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();

		private long numRecords = 0;
		private String inputFile;
		
		private Wsjni ws = null;
		
		public void configure(JobConf job) {
			caseSensitive = job.getBoolean("collect.casesensitive", true);
			inputFile = job.get("map.input.file");
			
			if (job.getBoolean("collect.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(job);
				} catch (IOException ioe) {
					System.err
							.println("Caught exception while getting cached files: "
									+ StringUtils.stringifyException(ioe));
				}
				if (patternsFiles != null){
					for (Path patternsFile : patternsFiles) {
						parseSkipFile(patternsFile);
					}
				}
			}
			if(job.getBoolean("collect.segmentation", false)){
				ws = new Wsjni();
				if(ws.ws_init("./ws/taobao.conf") != 0){
					System.out.println("fail to init ws. ");
					return;
				}
				else{
					System.out.println("Sucess init ws module!");
				}
				//ws.debug  = 1;
			}
		}

		private void parseSkipFile(Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader(
						patternsFile.toString()));
				String pattern = null;
				while ((pattern == fis.readLine())) {
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while parsing the cached file: "
								+ patternsFile
								+ " "
								+ StringUtils.stringifyException(ioe));
			}
		}

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable,Text> output, Reporter reporter)
				throws IOException {
			
				String[] records = value.toString().split("\t");
				if (records.length == 4) {
					String txtValue = (caseSensitive) ? records[3]:records[3].toLowerCase();
					txtValue = txtValue.trim();
					for (String pattern : patternsToSkip) {
						txtValue = txtValue.replaceAll(pattern, " ");
					}
					String [] words = (String[])ws.ws_segment_arr(txtValue);
					StringBuilder sb = new StringBuilder();
					if(words == null){
						System.err.println("Error for the segmentation: "+txtValue);
					}
					else{
						for(int j=0; j<words.length; j++){
							sb.append(words[j]);
							sb.append(" ");
						}
					}
					recordValue.set(sb.toString());
					int idKey = Integer.parseInt(records[2]);
					output.collect(new IntWritable(idKey), recordValue);
					reporter.incrCounter(Counters.Items, 1);
				}	
				if ((++numRecords % 100) == 0) {
					reporter.setStatus("Finished processing " + numRecords
							+ " records" + "from the input file:" + inputFile);
				}
		}
		
		public void close(){
			if(ws != null) {
				ws.ws_destroy();
			}
		}
	}

	public static class ReduceClass extends MapReduceBase implements
			Reducer<IntWritable, Text, IntWritable, Text> {
		
		private Text recordValue = new Text();
		
		public void reduce(IntWritable key, Iterator<Text> values,
				OutputCollector<IntWritable,Text> output, Reporter reporter)
				throws IOException {
			StringBuffer sb = new StringBuffer();
			while (values.hasNext()) {
				sb.append(values.next().toString());
				sb.append(" ");
			}
			recordValue.set(sb.toString());
			output.collect(key, recordValue);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CollectCatsOfShop.class);
		conf.setJobName("CollectCatsOfShop");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setCombinerClass(ReduceClass.class);
		conf.setReducerClass(ReduceClass.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setNumReduceTasks(3);

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache
						.addCacheFile(new Path(args[++i]).toUri(), conf);
				conf.setBoolean("collect.skip.patterns", true);
			} else {
				other_args.add(args[i]);
			}
		}

		//Path tempDir = new Path(other_args.get(1) + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		conf.set("mapred.child.java.opts","-Djava.library.path=./ws");     
	    DistributedCache.createSymlink(conf);
	    DistributedCache.addCacheArchive(new URI("/group/tbsc-dev/laisheng/ws_0.9.6.3-1.1.x86.jar#ws"),conf);

		
		try {
			FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));

			conf.setOutputFormat(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

			JobClient.runJob(conf);

			// Sort the output to get the topn result;
			//JobConf sortJob = new JobConf(getConf(), GetTopNFields.class);
			//sortJob.setJobName("Sort Fields");
			/*sortJob.setMapperClass(InverseMapper.class);
			sortJob.setNumReduceTasks(1);

			sortJob.setInputFormat(SequenceFileInputFormat.class);
			sortJob.setOutputFormat(TextOutputFormat.class);
			sortJob.setOutputKeyClass(IntWritable.class);
			sortJob.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(sortJob, tempDir);
			FileOutputFormat.setOutputPath(sortJob, new Path(other_args.get(1)));
			JobClient.runJob(sortJob);*/
		} finally {
			//FileSystem.get(conf).delete(tempDir);
			
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner
				.run(new Configuration(), new CollectCatsOfShop(), args);
		System.exit(ret);
	}

}