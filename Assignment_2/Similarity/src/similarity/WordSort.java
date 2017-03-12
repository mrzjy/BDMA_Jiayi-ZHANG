package similarity;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordSort extends Configured implements Tool {
   private enum LineCounter{
	   numLines
   }
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      conf.set("StopWordsFileName", args[2]);
      conf.set("WordFreqFileName", args[3]);
      conf.set("mapreduce.map.output.compress", "true");
      int res = ToolRunner.run(conf, new WordSort(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "WordSort");
      job.setJarByClass(WordSort.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setNumReduceTasks(1);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      private Text words = new Text();
      public List<Text> stopWords = new ArrayList<Text>(); 
      public HashMap<String,Integer> wordFreq = new HashMap<String,Integer>(); 

      public void loadWordFreq(String filename) throws IOException{
    	  Path path = new Path(filename);//Location of wordfreq file
          FileSystem fs = FileSystem.get(new Configuration());
    	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    		  
    	  String sCurrentLine;
    	      
          while ((sCurrentLine = br.readLine()) != null) {
        	  String[] wordfreq = sCurrentLine.split("[\\s]+");
        	  this.wordFreq.put(wordfreq[0], new Integer(wordfreq[1]));
    	  }
          
          br.close();
          
          return;
       }

      public void loadStopWords(String filename) throws IOException{
    	  Path pt=new Path(filename);//Location of stopwords.csv
          FileSystem fs = FileSystem.get(new Configuration());
    	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    		  
    	  String sCurrentLine;
    	      
          while ((sCurrentLine = br.readLine()) != null) {
        	  String stopWord = sCurrentLine.replaceAll("[^A-Za-z0-9]+", "");
        	  Text t = new Text();
        	  t.set(stopWord);
        	  this.stopWords.add(t);
    	  }
          
          br.close();
          
          return;
       }
      
      public void setup(Context context) throws IOException,InterruptedException { 
    	 super.setup(context);
    	 String filename = context.getConfiguration().get("StopWordsFileName");
    	 loadStopWords(filename);
    	 loadWordFreq(context.getConfiguration().get("WordFreqFileName"));
    	 
      }
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 //add count to counter numLines
    	 Counter counter = context.getCounter(LineCounter.numLines);
    	 counter.increment(1);
    	 Set<String> wordSet = new HashSet<String>();
    	 if (value.toString().isEmpty()){
    		 return;
    	 }
         for (String token: value.toString().split("\\s+|-{2,}+")) {
        	 String s = token.replaceAll("[^A-Za-z0-9]+", "");

        	 wordSet.add(s);
         }
         List<String> wordList = new ArrayList<String>(wordSet);
         
         //define sort method
         Collections.sort(wordList, new Comparator<String>() {
        	 @Override
        	 public int compare(String s1, String s2)
        	 {
        		 return  wordFreq.get(s1).compareTo(wordFreq.get(s2));
        	 }
         });
         words.set(StringUtils.join(wordList,","));
         context.write(new LongWritable(counter.getValue()), words);
      }
   }

   public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 Text words = new Text();
    	 for (Text v : values){
    		 words.set(v);
    	 }
         context.write(key, words);
      }
   }
}

