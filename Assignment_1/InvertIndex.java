package edu.stanford.cs246.wordcount;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import org.apache.hadoop.io.ArrayWritable;
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

public class InvertIndex extends Configured implements Tool {	
   // this is my counters
   private enum MyCounter {
		  PG100,
		  PG31100,
		  PG3200
   }
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      //from now on the first argument would be a repository where we stock all the 3 txt together, which is more practical than before
      Configuration conf = new Configuration();
      conf.set("StopWordsFile", args[2]);
      // set the 3rd argument to the variable StopWordsFileName
      int res = ToolRunner.run(conf, new InvertIndex(), args);
	  
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertIndex");
      job.setJarByClass(InvertIndex.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text[].class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
//      job.setCombinerClass(Reduce.class);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      Integer i;
      // set correspond values to every my counters
      PrintWriter writer = new PrintWriter(args[3], "UTF-8");
      i = (int) job.getCounters().findCounter(MyCounter.PG100).getValue();
      writer.println("PG100: "+i.toString()+"\n");
      i = (int) job.getCounters().findCounter(MyCounter.PG31100).getValue();
      writer.println("PG31100: "+i.toString()+"\n");
      i = (int) job.getCounters().findCounter(MyCounter.PG3200).getValue();
      writer.println("PG3200: "+i.toString()+"\n");
      writer.close();
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      private Text word = new Text();
      private Text filename = new Text();
      public List<Text> stopWords = new ArrayList<Text>(); 
      
      public void loadStopWords(String filename) throws IOException{
    	  Path path=new Path(filename);
          FileSystem fs = FileSystem.get(new Configuration());
    	  BufferedReader BR = new BufferedReader(new InputStreamReader(fs.open(path)));
    		  
    	  String sCurrentLine;
    	      
          while ((sCurrentLine = BR.readLine()) != null) {
        	  String stopWord = sCurrentLine.replaceAll("[^A-Za-z]+", "");
        	  Text text = new Text();
        	  text.set(stopWord);
        	  stopWords.add(text);
    	  }
          
          BR.close();
          
          return;
       }
      
      public void setup(Context context) throws IOException,InterruptedException { 
    	 super.setup(context);
    	 String filename = context.getConfiguration().get("StopWordsFile");
    	 loadStopWords(filename);
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 FileSplit fileSplit = (FileSplit)context.getInputSplit();
    	 String name = fileSplit.getPath().getName();
    	 //String one = String.valueOf(1);
    	 filename.set(name);
         for (String token: value.toString().split("\\s+|-{2,}+")) {
        	// delete all chars that are not alphabet and transfer capital letters to lower case
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
         ArrayList<Text> res = new ArrayList<Text>();
         for (Text val : values) {
        	
        	if (!res.contains(val)){
        		res.add(new Text(val));
        	}
         }
         if (res.size()==1){
        	 String filename = res.get(0).toString();
        	 switch (filename){
	        	 case "pg100.txt":
	        		 context.getCounter(MyCounter.PG100).increment(1);
	        		 break;
	        	 case "pg31100.txt":
	        		 context.getCounter(MyCounter.PG31100).increment(1);
	        		 break;
	        	 case "pg3200.txt":
	    		     context.getCounter(MyCounter.PG3200).increment(1);
	    		     break;
	         }
         }
         Set<Text> filenames = new HashSet<Text>(res);
         String output = new String();
         // store the documents where appears the word 
         for (Text txt : filenames){
        	 output += txt.toString();
        	 output += ',';
         }
         output = output.substring(0, output.length()-1);
         Text Output = new Text();
         // transform output from string to text
         Output.set(output);
         context.write(key, Output);
      }
   }
   
      
}