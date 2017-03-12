package edu.stanford.cs246.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertIndexExten extends Configured implements Tool {	
   
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Configuration conf = new Configuration();
      conf.set("StopWordsFileName", args[2]);
      // set the 3rd argument to the variable StopWordsFileName
      int res = ToolRunner.run(conf, new InvertIndexExten(), args);
	  
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertIndexExten");
      job.setJarByClass(InvertIndexExten.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setNumReduceTasks(10);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text();
      private Text filename = new Text();
      public List<Text> stopWords = new ArrayList<Text>(); 
      
      public void loadStopWords(String filename) throws IOException{
    	  Path pt=new Path(filename);//Location of file in HDFS
          FileSystem fs = FileSystem.get(new Configuration());
    	  BufferedReader BR = new BufferedReader(new InputStreamReader(fs.open(pt)));
    		  
    	  String CurrentLine;
    	      
          while ((CurrentLine = BR.readLine()) != null) {
        	  String stopWord = CurrentLine.replaceAll("[^A-Za-z]+", "");
        	  Text txt = new Text();
        	  txt.set(stopWord);
        	  stopWords.add(txt);
    	  }
          
          BR.close();
          
          return;
       }
      
      public void setup(Context context) throws IOException,InterruptedException { 
    	 super.setup(context);
    	 String filename = context.getConfiguration().get("StopWordsFileName");
    	 loadStopWords(filename);
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	 String name = fileSplit.getPath().getName();
    	 filename.set(name);
         for (String token: value.toString().split("\\s+|-{2,}+")) {
            word.set(token.replaceAll("[^A-Za-z]+", "").toLowerCase());
            if (!stopWords.contains(word)){
               context.write(word, filename);            	
            }
         }
      }
   }

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
         List<Text> res = new ArrayList<Text>();
         for (Text val : values) {
        	res.add(new Text(val));
         }
         Set<Text> filenames = new HashSet<Text>(res);
         String output = new String();
         // store the documents where appears the word 
         for (Text txt : filenames){
        	 output += txt.toString()+'#'+Collections.frequency(res, txt);
        	 // Collections.frequency(res, txt) would return directly the word frequency of txt in res
        	 output += ',';
         }
         output = output.substring(0, output.length()-1);
         Text Output = new Text();
         Output.set(output);
         context.write(key, Output);
      }
   }   
}