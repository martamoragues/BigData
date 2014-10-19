package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount_E3 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Float skipModule = null;
    private float limit_to_read;
    private long start_block; 
    private long size_all_data;
    private long current_read;

    protected void setup(Context context){
        start_block = ((FileSplit)context.getInputSplit()).getStart();
        current_read = start_block;
        skipModule = context.getConfiguration().getFloat("P", -1.0f);
        size_all_data = context.getConfiguration().getLong("size_all_data", -1);
        limit_to_read = size_all_data*skipModule;
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        int line_length= value.getLength();
        if(current_read < limit_to_read){    
            current_read += (line_length + 1);
            StringTokenizer itr = new StringTokenizer(value.toString());

        	  while (itr.hasMoreTokens()) {                  
                	word.set(itr.nextToken());
                	context.write(word, one);
            }
        }
	 }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 4) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    float P = Float.parseFloat(otherArgs[2]);
    long size_all_data = Long.parseLong(otherArgs[3]);

    conf.setFloat("P", P);
    conf.setLong("size_all_data", size_all_data);

    Job job = new Job(conf, "word count E3 P: "+P+ " size: " +size_all_data);
    job.setJarByClass(WordCount_E3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
                                                                    
