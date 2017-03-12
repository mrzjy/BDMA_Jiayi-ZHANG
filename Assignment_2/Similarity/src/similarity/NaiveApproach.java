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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NaiveApproach extends Configured implements Tool {
   private enum pairCounter{
      numPair
   }
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      Long line = loadFileLength(args[2]);
      conf.set("DocumentlastID", line.toString());
      conf.set("mapreduce.map.output.compress", "true");
      conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(600));
      int res = ToolRunner.run(conf, new NaiveApproach(), args);
      
      System.exit(res);
   }
   
   public static Long loadFileLength(String filename) throws IOException{
 	  Path path = new Path(filename);
      FileSystem fs = FileSystem.get(new Configuration());
 	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
 	  
 	  Long l = Long.parseLong(br.readLine());
       
      br.close();
       
      return l;
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "NaiveApproach");
      job.setJarByClass(NaiveApproach.class);

      job.setMapOutputKeyClass(LongPair.class);
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
   
   public static class Map extends Mapper<Text, Text, LongPair, Text> {
      private Text content = new Text();
      private LongPair DocPair = new LongPair();
      private Long lastID;
      
      public void setup(Context context) throws IOException,InterruptedException { 
    	  super.setup(context);
    	  this.lastID = Long.parseLong(context.getConfiguration().get("DocumentlastID"));
      }
      
      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 if (value.toString().isEmpty()){
    		 return;
    	 }
    	 
    	 String keyOut = key.toString();

    	 this.content = value;
    	 this.DocPair.setFirst(Long.parseLong(keyOut));
    	 
    	 long counter = 1;
    	 
    	 while(counter<=this.lastID){
    		 this.DocPair.setSecond(counter);
    		 
    		 if (this.DocPair.getDiff()==0){
    			 counter += 1;
    			 continue;
    		 }
    		 
    		 context.write(DocPair,content);
    		 counter += 1;
    	 }
      }
   }
    public static class Reduce extends Reducer<LongPair, Text, Text, Text> {
	    
	   private Set<String> text2Set(String s){
		   return new HashSet<String>(Arrays.asList(s.split(",")));
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
      public void reduce(LongPair key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 int counter = 0;
		 String[] strings =  new String[2];
    	 for (Text val : values){
    		 strings[counter] = val.toString();
    		 counter += 1;
    	 }
    	     	 
    	 if (counter!=2){ // this means that document id is not in input file 
    		return;
    	 }
    	 
    	 double s = similarity(strings[0], strings[1]);
    	 context.getCounter(pairCounter.numPair).increment(1);
    	 
    	 if (s>=0.8){
             context.write(new Text(key.toString()), new Text(String.valueOf(s))); 
    	 }
      }
   }
}

