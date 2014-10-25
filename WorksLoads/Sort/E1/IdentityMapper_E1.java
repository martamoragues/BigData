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

//package org.apache.hadoop.mapred.lib;
package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.Random;
import java.lang.Math;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

/** Implements the identity function, mapping inputs directly to outputs. 
 * @deprecated Use {@link org.apache.hadoop.mapreduce.Mapper} instead.
 */
@Deprecated
public class IdentityMapper_E1<K, V>
    extends MapReduceBase implements Mapper<K, V, K, V> {

	 private Random rnd = null;
     private Integer P = null;
	 public void configure(JobConf job){
        rnd = new Random(job.getInt("seed", -1));
        P = job.getInt("P", -1);
     }
  /** The identify function.  Input key/value pair is written directly to
   * output.*/
  public void map(K key, V val,
                  OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
    	int random = rnd.nextInt(100);
		if(random > P) {
                return;
        }
		output.collect(key, val);
  }
}
