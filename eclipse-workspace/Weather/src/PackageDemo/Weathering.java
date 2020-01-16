package PackageDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Weathering {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "Weather");
		j.setJarByClass(Weathering.class);
		j.setMapperClass(MapForWeather.class);
		j.setReducerClass(ReduceForWeather.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForWeather extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final int MISSING= 9999;
		@Override
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String line = value.toString();
			String year = line.substring(15,19);
			int airTemperature;
			if(line.charAt(87)=='+') 
				airTemperature = Integer.parseInt(line.substring(88,92));
			else
				airTemperature = Integer.parseInt(line.substring(87,92));
			String quality = line.substring(92,93);
			if(airTemperature!=MISSING&& quality.matches("[01459]"))
				con.write(new Text(year), new IntWritable(airTemperature));
		}
	}

	public static class ReduceForWeather extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> value, Context con)
				throws IOException, InterruptedException {

		}
	}
}
