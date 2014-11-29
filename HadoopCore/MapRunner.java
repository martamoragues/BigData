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

package org.apache.hadoop.mapred;

import java.util.*;
import java.util.Random;
import java.io.IOException;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

/** Default {@link MapRunnable} implementation.*/
public class MapRunner<K1, V1, K2, V2>
    implements MapRunnable<K1, V1, K2, V2> {
  
  private Mapper<K1, V1, K2, V2> mapper;
  private boolean incrProcCount;
  private JobConf job;

  @SuppressWarnings("unchecked")
  public void configure(JobConf job) {
    this.job = job;
    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
    //increment processed counter only if skipping feature is enabled
    this.incrProcCount = SkipBadRecords.getMapperMaxSkipRecords(job)>0 && 
      SkipBadRecords.getAutoIncrMapperProcCount(job);
  }

  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
                  Reporter reporter)
    throws IOException {
    try {
      // allocate key & value instances that are re-used for all entries
      K1 key = input.createKey();
      V1 value = input.createValue();
      int id = TaskAttemptID.forName(job.get("mapred.task.id")).getTaskID().getId();
	  int estrategia;
	  try {
	      estrategia = getConfInt("sampling.estrategia", job);
	  } catch (IOException e) {
	      estrategia = 0;
	  }

     if(estrategia == 1)
    {
      System.out.println("E1: " + getConfInt("sampling.estrategia", job) + " seed: " + getConfInt("sampling.seed", job)+ " ID: " + id + " P: " + getConfInt("sampling.P", job) );
      Random rnd = new Random(getConfInt("sampling.seed", job) + id);
      int random;
      int P = getConfInt("sampling.P", job);


	  while (input.next(key, value)) {
        // map pair to output
     	random = rnd.nextInt(100);  
		if(random < P){
			mapper.map(key, value, output, reporter);
		}	
		if(incrProcCount) {
          	reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
            SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
		
      }
    }	


    else if(estrategia == 2)
    {
      System.out.println("E2: " + getConfInt("sampling.estrategia", job) + " seed: " + getConfInt("sampling.seed", job)+ " ID: " + id + " P: " + getConfFloat("sampling.P", job) );
      Random rnd = new Random(getConfInt("sampling.seed", job) + id);
      Float skipModule = getConfFloat("sampling.P", job);

      double p = 1.0/skipModule;
      Double prop = Math.log(1.0-p); // -0.69
      Double result = num_random(prop, rnd);
		
	  while (input.next(key, value)) {
        // map pair to output
       if(result == 0.0){
		 mapper.map(key, value, output, reporter);
		 result = num_random(prop, rnd);
       }
	   else {
			result -= 1.0;
	   }
       if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      }
    }


	else if(estrategia == 3)
    {
      System.out.println("E3: " + getConfInt("sampling.estrategia", job) + " file size: " + getConfLong("sampling.all.file.size", job)+ " P: " + getConfFloat("sampling.P", job) );
      long size_all_data = getConfLong("sampling.all.file.size", job);
      Float skipModule = getConfFloat("sampling.P", job);
      long current_read=((org.apache.hadoop.mapred.FileSplit)reporter.getInputSplit()).getStart();
      float limit_to_read = size_all_data*skipModule;
//	 System.out.println("Current read: " + current_read + " limit: " + limit_to_read);
	  if(current_read < limit_to_read){
//		 System.out.println("ENTRO IF");
	    while (input.next(key, value)) {
          // map pair to output
           mapper.map(key, value, output, reporter);
           if(incrProcCount) {
              reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
           }
        }
	  }
    }


	else if(estrategia == 4)
    {
      System.out.println("E4: " + getConfInt("sampling.estrategia", job) +" P: " + getConfFloat("sampling.P", job) );
      Float P = getConfFloat("sampling.P", job);

	  while (input.next(key, value)) {
        // map pair to output
       if(input.getProgress()<=P){
		 mapper.map(key, value, output, reporter);
       }
	   else {
			break;	
       }
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      }
    }


	else if(estrategia == 5)
    {
      System.out.println("E5: " + getConfInt("sampling.estrategia", job) + " P: " + getConfInt("sampling.P", job) );
      long start_block = ((org.apache.hadoop.mapred.FileSplit)reporter.getInputSplit()).getStart();
      int skipModule = getConfInt("sampling.P", job);
      long size_block = getConfInt("dfs.block.size", job);
      long num_bloc = start_block / size_block;
      boolean result = ((num_bloc%skipModule)==0);

  //    System.out.println("Bloc: " + num_bloc + " result: " + result);
      if(result){
	//	 System.out.println("ENTRO IF");
		while (input.next(key, value)) {
        	// map pair to output
        	mapper.map(key, value, output, reporter);
        	if(incrProcCount) {
          		reporter.incrCounter(SkipBadRecords.COUNTER_GROUP,
              	SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        	}
      	}

      }
    }


	else{ 
      while (input.next(key, value)) {
        // map pair to output
        mapper.map(key, value, output, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      }
	}
    } finally {
      mapper.close();
    }
  }

	private int getConfInt(String name, JobConf job) throws IOException {
    return Integer.parseInt(getConf(name, job));
  }

  private float getConfFloat(String name, JobConf job) throws IOException {
    return Float.parseFloat(getConf(name, job));
  }

  private long getConfLong(String name, JobConf job) throws IOException {
    return Long.parseLong(getConf(name, job));
  }

  private String getConf(String name, JobConf job) throws IOException {
    String result = job.get(name);
    if (result == null) {
      throw new IOException("Missing required sampling parameters");
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

  protected Mapper<K1, V1, K2, V2> getMapper() {
    return mapper;
  }
}
