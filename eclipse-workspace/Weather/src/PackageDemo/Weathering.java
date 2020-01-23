package PackageDemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Weathering {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage:MaxTemperature<input path><output path>");
			System.exit(-1);
		}
//		 @SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(Weathering.class);
		job.setJobName("Max temperature");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MapForWeather.class);
		job.setReducerClass(ReduceForWeather.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:54310/user/hduser/");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(new Path(args[1]));
		// copy hdfsoutput file to local folder
		for (int i = 0; i < status.length; i++) {
			System.out.println(status[i].getPath());
			fs.copyToLocalFile(false, status[i].getPath(), new Path("/home/hduser/" + args[1]));
		}
		System.out.println("\nYear\tTemperature\n");
		// display contents of local file
		BufferedReader br = new BufferedReader(new FileReader("/home/hduser/" + args[1]));
		String line = null;
		while ((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
		Scanner s = new Scanner(new File("/home/hduser/" + args[1]));
		List<Integer> temps = new ArrayList<Integer>();
		List<String> years = new ArrayList<String>();
		while (s.hasNext()) {
			years.add(s.next());
			temps.add(Integer.parseInt(s.next()));
		}
		int max_temp = 0, min_temp = 999, i = 0, j = 0;
		String hottest_year = "", coolest_year = "";
		for (int temp : temps) {
			if (temp > max_temp) {
				max_temp = temp;
				hottest_year = years.get(i);
			}
			i++;
		}
		float max_temp1 = max_temp;
		System.out.println("Hottest Year:" + hottest_year);
		System.out.println("\tTemperature:" + max_temp1 / 10 + " Degree Celcius");
		for (int temp : temps) {
			if (temp < min_temp) {
				min_temp = temp;
				coolest_year = years.get(j);
			}
			j++;
		}
		float min_temp1 = min_temp;
		System.out.println("Coolest Year:" + coolest_year);
		System.out.println("\tTemperature:" + min_temp1 / 10 + " Degree Celcius");
		s.close();
	}

	public static class MapForWeather extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final int MISSING = 9999;
		@Override
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87) == '+')
				airTemperature = Integer.parseInt(line.substring(88, 92));
			else
				airTemperature = Integer.parseInt(line.substring(87, 92));
			String quality = line.substring(92, 93);
			if (airTemperature != MISSING && quality.matches("[01459]"))
				con.write(new Text(year), new IntWritable(airTemperature));
		}
	}

	public static class ReduceForWeather extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values)
				maxValue = Math.max(maxValue, value.get());
			con.write(word, new IntWritable(maxValue));
		}
	}
}
