//package org.marta;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// https://stackoverflow.com/questions/5377118/how-to-convert-txt-file-to-hadoops-sequence-file-format

public class SeqConverter {

    public static void main(String[] args) throws IOException,
        InterruptedException, ClassNotFoundException {

    Configuration conf = new Configuration();
    JobConf job = new JobConf(conf, SeqConverter.class);
    job.setJobName("Convert Text");
    //job.setJarByClass(Mapper.class);

    //job.setMapperClass(Mapper.class);
    //job.setReducerClass(Reducer.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(IdentityReducer.class);

    // increase if you need sorting or a special number of files
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //job.setOutputFormatClass(SequenceFileOutputFormat.class);
    //job.setInputFormatClass(TextInputFormat.class);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    //TextInputFormat.addInputPath(job, new Path("/lol"));
    //SequenceFileOutputFormat.setOutputPath(job, new Path("/lolz"));
Path input = new Path("input.txt");
Path output = new Path("output.txt");

SequenceFileInputFormat.addInputPath(job, input);
TextOutputFormat.setOutputPath(job, output);

    // submit and wait for completion
    //job.waitForCompletion(true);
JobClient.runJob(job);
   }

}

