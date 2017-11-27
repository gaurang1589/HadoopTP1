import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Question2_1 {
	
	public static class FlickrMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String words []=new String[23];
			words = value.toString().split("\\t");
			Country pays= Country.getCountryAt(Double.parseDouble(words[11]),Double.parseDouble(words[10]));
			if (pays!=null)
				for(String word: words[8].toString().split(",")){
					
					context.write(new Text (pays.toString()),new Text(URLDecoder.decode(word, "UTF-8")));
				}
			
			
		}
	}

	public static class FlickrReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				HashMap<String,Integer> hm =new HashMap<String,Integer>();
				
				int k=Integer.parseInt(context.getConfiguration().get("k"));
				PriorityQueue<StringAndInt> pq=new PriorityQueue<StringAndInt>(k);
				
				for (Text val : values) {
					if (! hm.containsKey(val)){
						hm.put(val.toString(), 1);
					}else {
						hm.put(val.toString(), hm.get(val)+1);
					} 
			      }
				for (Map.Entry<String, Integer> entry : hm.entrySet()){
						pq.add(new StringAndInt(entry.getKey(),entry.getValue()));
				}
				context.write(key,new Text(pq.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k= otherArgs[2];
		
		conf.set("k", k);
		
		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);
		
		job.setMapperClass(FlickrMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//job.setCombinerClass(WordCountReducer.class);
		//
		
		job.setReducerClass(FlickrReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
	
}
