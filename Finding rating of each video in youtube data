import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class Video_rating {

    public static class Map extends Mapper<LongWritable, Text, Text,
FloatWritable> {

       private Text video_name = new Text();
       private  FloatWritable rating = new FloatWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split("\t");

          if(str.length > 7){
                video_name.set(str[0]);
                if(str[6].matches("\\d+.+")){ //this regular expression
specifies that the string should contain only floating values
                float f=Float.parseFloat(str[6]); //typecasting string to float
                rating.set(f);
                }
          }

      context.write(video_name, rating);
      }

    }

    public static class Reduce extends Reducer<Text, FloatWritable,
Text, FloatWritable> {

       public void reduce(Text key, Iterable<FloatWritable> values,
Context context)
         throws IOException, InterruptedException {
           float sum = 0;
           int l=0;
           for (FloatWritable val : values) {
                   l+=1;  //counts number of values are there for that key
               sum += val.get();
           }
           sum=sum/l;   //takes the average of the sum
           context.write(key, new FloatWritable(sum));
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "videorating");
           job.setJarByClass(Video_rating.class);

           job.setMapOutputKeyClass(Text.class);
           job.setMapOutputValueClass(FloatWritable.class);
      //job.setNumReduceTasks(0);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(FloatWritable.class);

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
