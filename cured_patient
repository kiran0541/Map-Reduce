import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class Cured_patient {

    public static class Map extends Mapper<LongWritable, Text, Text,
IntWritable> {

       private Text state = new Text();
       private IntWritable discharged = new IntWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");


           if(str.length==12 ){
           state.set(str[5]);
           if(str[8].matches("\\d+")){          //check for digits in string
                   int i=Integer.parseInt(str[8]);
           discharged.set(i);
           }
              context.write(state, discharged);

           }


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
           //if(key.toString().equals("AL"))
           context.write(key, new IntWritable(sum));
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "wordcount");
           job.setJarByClass(Cured_patient.class);
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(IntWritable.class);

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
