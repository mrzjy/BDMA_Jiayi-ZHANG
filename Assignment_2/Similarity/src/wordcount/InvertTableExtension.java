package wordcount;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.KeyStore.LoadStoreParameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
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

public class InvertTableExtension extends Configured implements Tool {	
   
	
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Configuration conf = new Configuration();
      conf.set("StopWordsFileName", args[2]);
//      conf.set("mapreduce.map.output.compress", "true");
      int res = ToolRunner.run(conf, new InvertTableExtension(), args);
	  
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertTableExtension");
      job.setJarByClass(InvertTableExtension.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Reduce.class);

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
    	  BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
    		  
    	  String sCurrentLine;
    	      
          while ((sCurrentLine = br.readLine()) != null) {
        	  String stopWord = sCurrentLine.replaceAll("[^A-Za-z]+", "");
        	  Text t = new Text();
        	  t.set(stopWord);
        	  stopWords.add(t);
    	  }
          
          br.close();
          
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
         Set<Text> unique = new HashSet<Text>(res);
         String output = new String();
         for (Text u : unique){
        	 output += u.toString()+'#'+Collections.frequency(res, u);
        	 output += ',';
         }
         output = output.substring(0, output.length()-1);
         Text value = new Text();
         value.set(output);
         context.write(key, value);
      }
   }   
   public static class TextArray extends ArrayWritable {
	    public TextArray(Text[] arr) {
	        super(Text.class);
	    }

	    @Override
	    public Text[] get() {
	        return (Text[]) super.get();
	    }

	    @Override
	    public void write(DataOutput arg0) throws IOException {
	        for(Text data : get()){
	            data.write(arg0);
	        }
	    }
	    
	    @Override
	    public String toString() {
	        Text[] values = get();
	        String output = new String();
	        for (Text t: values){
		        output += t.toString();
		        output += ",";
	        }
	        output = output.substring(0, output.length()-1);
	        return output;
	    }
   }
}



