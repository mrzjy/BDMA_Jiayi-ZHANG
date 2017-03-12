package similarity;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.text.Document;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PrefixFilterApproach extends Configured implements Tool {
   private enum pairCounter{
	   numPair
   }
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      conf.set("mapreduce.map.output.compress", "true");
      conf.set("Doc",args[0]);
      int res = ToolRunner.run(conf, new PrefixFilterApproach(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "PrefixFilterApproach");
      job.setJarByClass(PrefixFilterApproach.class);

      job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
      job.setInputFormatClass(KeyValueTextInputFormat.class);
	  job.setOutputFormatClass(TextOutputFormat.class);
	  
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setNumReduceTasks(1);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<Text, Text, Text, Text> {
      private Text word = new Text();

      
      public void setup(Context context) throws IOException,InterruptedException { 
    	  super.setup(context);
      }
      
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 if (value.toString().isEmpty()){
    		 return;
    	 }
    	 
    	 String[] doc = value.toString().split(",");
    	 int numFirstWords = doc.length - (int)Math.floor(doc.length*0.8);
    	 int counter = 0;
    	 
    	 while(counter<numFirstWords){
    		 word.set(doc[counter]);
    		 context.write(word,key);
    		 counter += 1;
    	 }
      }
   }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
    	public HashMap<Text,Text> doc = new HashMap<Text,Text>(); 
	    
	   private Set<String> text2Set(String s){
		   return new HashSet<String>(Arrays.asList(s.split(",")));
	   }
	   
	   @Override
       public void setup(Context context) throws IOException,InterruptedException { 
     	 super.setup(context);
     	 String filename = context.getConfiguration().get("Doc");
     	 loadDocument(filename);
       }
	   
	   public void loadDocument(String filename) throws IOException{
		 	  Path path = new Path(filename);
		      FileSystem fs = FileSystem.get(new Configuration());
		 	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
		 	  
	    	  String currentLine;
    	      
	          while ((currentLine = br.readLine()) != null) {
	        	  String[] document = currentLine.split("[\\s]+");
	        	  doc.put(new Text(document[0]), new Text(document[1]));
	    	  }
	   }
	   
	   public double similarity(String t1, String t2) {

		   Set<String> s1 = text2Set(t1);
		   Set<String> s2 = text2Set(t2);
		   
		   Set<String> union = new HashSet<String>(s1);
		   union.addAll(s2);
		   
		   Set<String> inter = new HashSet<String>(s1);
		   inter.retainAll(s2);
		   
		   if (union.size()==0){
			   return 0;
		   }
		   
		   return inter.size()/union.size();
	    }
	   
	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
		 List<Text> value = new ArrayList<Text>();
		 for (Text val :values){
			 value.add(new Text(val));
		 }
    	 for (Text val1 : value){
    		 for (Text val2: value){
    			 if (val1.equals(val2)){
    				 continue;
    			 }
    			 
    			 String s1 = this.doc.get(val1).toString();
    			 String s2 = this.doc.get(val2).toString();

    	    	 context.getCounter(pairCounter.numPair).increment(1);
    			 Double s = similarity(s1, s2);
    			 
    			 if (s>=0.8){
    				 context.write(new Text(val1.toString()+','+val2.toString()), new Text(String.valueOf(s)));
    			 }
    		 }
    	 }
      }
   }
}

