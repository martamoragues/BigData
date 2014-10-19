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

public class WordCount_E2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //aconseguim el 25% amb el 4
    private Float skipModule = null;
    private Random rnd = null;
    private Double result = null;
    private Double prop = null;

    public double num_random(double prop){
        double u = rnd.nextDouble();
        double k = Math.log(u)/prop;
//        System.out.println("logaritmo en random----> "+k);
        k = Math.floor(k);
        return (int)(k);
    }
    protected void setup(Context context){
       rnd = new Random(context.getConfiguration().getInt("seed", -1));
       skipModule = context.getConfiguration().getFloat("max", -1.0f);
       double p = 1.0/skipModule;
       prop = Math.log(1.0-p);
       result = num_random(prop);
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(result == 0.0){
        //                System.out.println("Entro al if----> ");
                        context.write(word, one);
                        result = num_random(prop);
          //             System.out.println("MORE Result of random ----> "+result);
                        continue;
                }
            //    System.out.println("fora del if----> "+result);
                result =result - 1.0;
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
    int seed = Integer.parseInt(otherArgs[2]);
    float max = Float.parseFloat(otherArgs[3]);
    System.out.println("El seed es "+seed+" i el max es "+max);

    conf.setFloat("max", max);
    conf.setInt("seed", seed);
    Job job = new Job(conf, "word count E2 seed: "+seed+ " max: " +max);
    job.setJarByClass(WordCount_E2.class);
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
                                                                                                                                       

