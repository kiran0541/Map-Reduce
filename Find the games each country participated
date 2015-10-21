//Find the games each country participated


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class Country_games {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

       private Text country = new Text();
       private Text games = new Text();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split("\t");
      if(str.length==10){

                country.set(str[2]);


       games.set(str[5]);
      }


               context.write(country, games);

       }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

       public void reduce(Text key, Iterable<Text> values, Context context)
         throws IOException, InterruptedException {

           Set<String> a=new  HashSet<String>();
           for (Text val : values) {

               a.add(val.toString());
           }

           context.write(key, new Text(a.toString()));

       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "country_games");
           job.setJarByClass(Country_games.class);

           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(Text.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);

       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);

       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
       job.waitForCompletion(true);
    }

  }
