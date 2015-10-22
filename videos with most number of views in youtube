import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class Video_views {

    public static class Map extends Mapper<LongWritable, Text, Text,
IntWritable> {

       private Text video = new Text();
       private IntWritable views = new IntWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split("\t");

          if(str.length > 6){
                video.set(str[0]);
                if(str[5].matches("\\d+")){  //this regular expression checks whether the data contain only integer values
                        int i=Integer.parseInt(str[5]); //type casting string to an int
                        views.set(i);
                }
          }
                     if(views.get()>1000) //if the number of views for that video>1000 then it will write into the context
                context.write(video, views);
      }

    }

    public static class Reduce extends Reducer<Text, IntWritable,
Text, IntWritable> {

       public void reduce(Text key, Iterable<IntWritable> values,
Context context)
         throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {

               sum += val.get();
           }
           context.write(key, new IntWritable(sum));
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "video_views");
           job.setJarByClass(Video_views.class);

           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(IntWritable.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

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
