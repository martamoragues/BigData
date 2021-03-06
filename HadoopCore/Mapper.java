/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
/** 
 * Maps input key/value pairs to a set of intermediate key/value pairs.  
 * 
 * <p>Maps are the individual tasks which transform input records into a 
 * intermediate records. The transformed intermediate records need not be of 
 * the same type as the input records. A given input pair may map to zero or 
 * many output pairs.</p> 
 * 
 * <p>The Hadoop Map-Reduce framework spawns one map task for each 
 * {@link InputSplit} generated by the {@link InputFormat} for the job.
 * <code>Mapper</code> implementations can access the {@link Configuration} for 
 * the job via the {@link JobContext#getConfiguration()}.
 * 
 * <p>The framework first calls 
 * {@link #setup(org.apache.hadoop.mapreduce.Mapper.Context)}, followed by
 * {@link #map(Object, Object, Context)} 
 * for each key/value pair in the <code>InputSplit</code>. Finally 
 * {@link #cleanup(Context)} is called.</p>
 * 
 * <p>All intermediate values associated with a given output key are 
 * subsequently grouped by the framework, and passed to a {@link Reducer} to  
 * determine the final output. Users can control the sorting and grouping by 
 * specifying two key {@link RawComparator} classes.</p>
 *
 * <p>The <code>Mapper</code> outputs are partitioned per 
 * <code>Reducer</code>. Users can control which keys (and hence records) go to 
 * which <code>Reducer</code> by implementing a custom {@link Partitioner}.
 * 
 * <p>Users can optionally specify a <code>combiner</code>, via 
 * {@link Job#setCombinerClass(Class)}, to perform local aggregation of the 
 * intermediate outputs, which helps to cut down the amount of data transferred 
 * from the <code>Mapper</code> to the <code>Reducer</code>.
 * 
 * <p>Applications can specify if and how the intermediate
 * outputs are to be compressed and which {@link CompressionCodec}s are to be
 * used via the <code>Configuration</code>.</p>
 *  
 * <p>If the job has zero
 * reduces then the output of the <code>Mapper</code> is directly written
 * to the {@link OutputFormat} without sorting by keys.</p>
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 * public class TokenCounterMapper 
 *     extends Mapper<Object, Text, Text, IntWritable>{
 *    
 *   private final static IntWritable one = new IntWritable(1);
 *   private Text word = new Text();
 *   
 *   public void map(Object key, Text value, Context context) throws IOException {
 *     StringTokenizer itr = new StringTokenizer(value.toString());
 *     while (itr.hasMoreTokens()) {
 *       word.set(itr.nextToken());
 *       context.collect(word, one);
 *     }
 *   }
 * }
 * </pre></blockquote></p>
 *
 * <p>Applications may override the {@link #run(Context)} method to exert 
 * greater control on map processing e.g. multi-threaded <code>Mapper</code>s 
 * etc.</p>
 * 
 * @see InputFormat
 * @see JobContext
 * @see Partitioner  
 * @see Reducer
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  public class Context 
    extends MapContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    public RecordReader<KEYIN,VALUEIN> input;
    public int taskID;
    public Context(Configuration conf, TaskAttemptID taskid,
                   RecordReader<KEYIN,VALUEIN> reader,
                   RecordWriter<KEYOUT,VALUEOUT> writer,
                   OutputCommitter committer,
                   StatusReporter reporter,
                   InputSplit split) throws IOException, InterruptedException {
      super(conf, taskid, reader, writer, committer, reporter, split);
    this.input = reader;
    this.taskID = taskid.getTaskID().getId();
    }
  }
  
  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Called once for each key/value pair in the input split. Most applications
   * should override this, but the default is the identity function.
   */
  @SuppressWarnings("unchecked")
  protected void map(KEYIN key, VALUEIN value, 
                     Context context) throws IOException, InterruptedException {
    context.write((KEYOUT) key, (VALUEOUT) value);
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }
  
  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   * @param context
   * @throws IOException
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    Configuration job = context.getConfiguration();
    int estrategia = getConfInt("sampling.estrategia", job);
    if(estrategia == 1)
    {
      System.out.println("E1: " + getConfInt("sampling.estrategia", job) + " seed: " + getConfInt("sampling.seed", job)+ " ID: " + context.taskID + " P: " + getConfInt("sampling.P", job) );
      Random rnd = new Random(getConfInt("sampling.seed", job) + context.taskID);
      int random;
      int P = getConfInt("sampling.P", job);

      while (context.nextKeyValue()) {
        random = rnd.nextInt(100);
        if(random < P){
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
      }
    }

    else if(estrategia == 2)
    {
      System.out.println("E2: " + getConfInt("sampling.estrategia", job) + " seed: " + getConfInt("sampling.seed", job)+ " ID: " + context.taskID + " P: " + getConfFloat("sampling.P", job) );
      Random rnd = new Random(getConfInt("sampling.seed", job) + context.taskID);
      Float skipModule = getConfFloat("sampling.P", job); 

      double p = 1.0/skipModule;
      Double prop = Math.log(1.0-p); // -0.69
      Double result = num_random(prop, rnd);

      while (context.nextKeyValue()) {
        // System.out.println("Current random: "+result);
        if(result == 0.0){
          // System.out.println("ENTRO IF");
          map(context.getCurrentKey(), context.getCurrentValue(), context);
          result = num_random(prop, rnd);
        } else {
          result -= 1.0;
        }
      }
    }

    else if(estrategia == 3)
    {
      System.out.println("E3: " + getConfInt("sampling.estrategia", job) + " file size: " + getConfLong("sampling.all.file.size", job)+ " P: " + getConfFloat("sampling.P", job) );
      long size_all_data = getConfLong("sampling.all.file.size", job);
      Float skipModule = getConfFloat("sampling.P", job);
      long current_read=((org.apache.hadoop.mapreduce.lib.input.FileSplit)context.getInputSplit()).getStart();
      float limit_to_read = size_all_data*skipModule;

      // System.out.println("Current read: " + current_read + " limit: " + limit_to_read);
      if(current_read < limit_to_read){
        // System.out.println("ENTRO IF");
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
      }
    }

    else if(estrategia == 4)
    {
      System.out.println("E4: " + getConfInt("sampling.estrategia", job) +" P: " + getConfFloat("sampling.P", job) );
      Float P = getConfFloat("sampling.P", job);
      while (context.nextKeyValue()) {
        //context.input.getProgress return the progress of the input read between 0.0 and 1.0
        // System.out.println("Progress: " + context.input.getProgress());
        if(context.input.getProgress()<=P){
          // System.out.println("LLEGEIXO");
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        } else {
          // System.out.println("SORTIM FORA");
          break;
        }
      }
    }

    else if(estrategia == 5)
    {
      System.out.println("E5: " + getConfInt("sampling.estrategia", job) + " P: " + getConfInt("sampling.P", job) );
      long start_block = ((org.apache.hadoop.mapreduce.lib.input.FileSplit)context.getInputSplit()).getStart();
      int skipModule = getConfInt("sampling.P", job);
      long size_block = getConfInt("dfs.block.size", job); 
      long num_bloc = start_block / size_block;
      boolean result = ((num_bloc%skipModule)==0);

      System.out.println("Bloc: " + num_bloc + " result: " + result);
      if(result){
        System.out.println("ENTRO IF");
        while (context.nextKeyValue()) {
          map(context.getCurrentKey(), context.getCurrentValue(), context);
        }
      } 
    }

    else {
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    }
    cleanup(context);
  }


  private int getConfInt(String name, Configuration job) throws InterruptedException {
    return Integer.parseInt(getConf(name, job));
  }

  private float getConfFloat(String name, Configuration job) throws InterruptedException {
    return Float.parseFloat(getConf(name, job));
  }

  private long getConfLong(String name, Configuration job) throws InterruptedException {
    return Long.parseLong(getConf(name, job));
  }

  private String getConf(String name, Configuration job) throws InterruptedException {
    String result = job.get(name);
    if (result == null) {
      throw new InterruptedException("Missing required sampling parameters");
    }
    return result;
  }


  public double num_random(double prop, Random rnd){
    double u = rnd.nextDouble();
    double k = Math.log(u)/prop;
    // System.out.println("nextDouble en random---> "+u);
    // System.out.println("logaritmo en random----> "+k);
    k = Math.floor(k);
    return (int)(k);
  }

}
