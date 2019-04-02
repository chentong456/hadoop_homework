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

// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2015

import java.io.IOException;
import java.util.StringTokenizer;
import java.math.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    //private final static IntWritable one = new IntWritable(1);
	//private DoubleWritable one;
   	private Text word = new Text();
    private Text one = new Text();  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	  int i=0;
	  String[] array = new String[itr.countTokens()];
	  while(itr.hasMoreTokens()){
	  	array[i]=itr.nextToken();
		i++;
	  }
	  if((array.length==3)&&(Double.parseDouble(array[2])>=0)){
	  	String a = array[0]+" "+array[1];
	  	word.set(a);
		//Double time=Double.parseDouble(array[2]);
	  	//DoubleWritable one=new DoubleWritable(time);
		one.set(array[2]);
	  	context.write(word, one);
	  }
	  //sum+=array[2];
      /*while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }*/
    }
  }
  
  public static class IntSumCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //double sum = 0;
	  double a=0;
	  BigDecimal sum=new BigDecimal(Double.toString(a));
	  int count=0;
      for (Text val : values) {
	  	BigDecimal ss=new BigDecimal(val.toString());
	  	//String tmp=val.toString();
		//double ss=Double.parseDouble(tmp);
        //sum += val.get();
		sum=sum.add(ss);
		count++;
      }
	  String outValue=""+sum+" "+count;
      result.set(outValue);
      context.write(key, result);
    }
  }

  // This is the Reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  //
  // We want to control the output format to look at the following:
  //
  // count of word = count
  //
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();
    private byte[] prefix;
    private byte[] suffix;

    protected void setup(Context context) {
      try {
        prefix= Text.encode("count of ").array();
        suffix= Text.encode(" =").array();
      } catch (Exception e) {
        prefix = suffix = new byte[0];
      }
    }

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double a=0;
	  BigDecimal  sum =new BigDecimal(Double.toString(a));
	  int count=0;
      for (Text val : values) {
	  	String mm=val.toString();
		String[] str=mm.split("\\s+");
		//double ss=Double.parseDouble(str[0]);
		BigDecimal ss=new BigDecimal(str[0].toString());
        //sum += ss;
		sum=sum.add(ss);
		Integer ii=Integer.valueOf(str[1]);
		count+=ii;
      }
	  //double f1=(double)Math.round(sum/count*1000)/1000;
	  //String kk=String.valueOf(count);
	  BigDecimal con=new BigDecimal(Integer.toString(count));
	  //BigDecimal hh=new BigDecimal(sum/con);
	  BigDecimal f1=sum.divide(con,3,RoundingMode.HALF_UP);
	  //Double f1=hh.setScale(3,BigDecimal.ROUND_HALF_UP).doubleValue();
	  String value=""+count+" "+f1;
	  //String value=""+sum;

      // generate result key
      result_key.set(prefix);
      result_key.append(key.getBytes(), 0, key.getLength());
      result_key.append(suffix, 0, suffix.length);

      // generate result value
      result_value.set(value);

      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "word count");

    job.setJarByClass(Hw2Part1.class);	
    //job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
