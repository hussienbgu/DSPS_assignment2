package com.amazonaws.dsps2021;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1 {

	public enum NCounter {
		N_ALL;
	};

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private String decade;
		private String w1;
		private String w2;
		private String count;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {
				String[] ngramEntry = itr.nextToken().split("\t");
				decade = ngramEntry[0];
				w1 = ngramEntry[1];
				w2 = ngramEntry[2];
				count = ngramEntry[3]; // c(w1w2)
				// key-value for counting c(w1)
				Text countKey = new Text(decade + "\t" + w1 + "\t" + "*");
				Text countValue = new Text(count);
				// key-value to match c(w1) with the rest of the N-gram data
				Text w2Key = new Text(decade + "\t" + w1 + "\t" + "**");
				Text w2Value = new Text(w2 + "\t" + count);
				// count N_
				context.getCounter("NCounter", "N_").increment(1);
				context.write(countKey, countValue);
				context.write(w2Key, w2Value);
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		private static Integer w1Sum = 0;
		private String current_w1;
		private String current_decade;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keysStrings = key.toString().split("\t");
			String w1, w2, count, year;
			if (keysStrings[2].equalsIgnoreCase("*")) {
				current_decade = keysStrings[0];
				current_w1 = keysStrings[1];
				w1Sum = 0;
				for (Text val : values) {
					w1Sum += Integer.parseInt(val.toString());
				}
			} else {
				year = keysStrings[0];
				w1 = keysStrings[1];
				for (Text val : values) {
					w2 = val.toString().split("\t")[0];
					count = val.toString().split("\t")[1];
					Text newKey = new Text(year + "\t" + w1 + "\t" + w2 + "\t" + count);
					Text newValue = new Text(w1Sum.toString());
					context.write(newKey, newValue);
				}
			}
		}
	}

	public static class CWcombiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keyString = key.toString().split("\t");
			if (keyString[2].equals("**")) {
				for (Text val : values) {
					context.write(key, val);
				}
			} else {
				Integer countSum = 0;
				for (Text val : values) {
					countSum += Integer.valueOf(val.toString());
				}
				context.write(key, new Text(countSum.toString()));
			}
		}
	}

	public static class CWPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] keyStr = key.toString().split("\t");
			String year = keyStr[0];
			String w = keyStr[1];
			int partitioner = ((year + w).hashCode()) % numReduceTasks;
			if (partitioner < 0) {
				return partitioner * -1;
			}
			return partitioner;
		}
	}

}
