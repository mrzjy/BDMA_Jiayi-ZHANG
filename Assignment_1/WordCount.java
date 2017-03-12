package edu.stanford.cs246.wordcount;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      //conf.set("mapreduce.map.output.compress", "true");
      
      // here the argument would be like : txt1 output1 txt2 output2 txt2 output2, which is improved in next two java codes
      String[] arg1 = new String[2];String[] arg2 = new String[2];String[] arg3 = new String[2];
      arg1[0] = args[0];arg1[1] = args[1];
      arg2[0] = args[2];arg2[1] = args[3];
      arg3[0] = args[4];arg3[1] = args[5];
      int res1 = ToolRunner.run(new Configuration(), new WordCount(), arg1);
      int res2 = ToolRunner.run(new Configuration(), new WordCount(), arg2);
      int res3 = ToolRunner.run(new Configuration(), new WordCount(), arg3);
      System.exit(res1);
      System.exit(res2);
      System.exit(res3);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "WordCount");
      job.setJarByClass(WordCount.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
      job.setCombinerClass(Reduce.class);
      job.setNumReduceTasks(10);
      
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
            word.set(token);
            context.write(word, ONE);
         }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
            sum += val.get();
         }
         if (sum > 4000){
        	 // select only the stoop words
         	context.write(key, new IntWritable(sum));
         }
      }
   }
}
