import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

   public class amountPerCity {

    public static class Map extends Mapper<LongWritable, Text, Text,
FloatWritable> {

       private Text disease = new Text();
       private FloatWritable cost  = new FloatWritable();
       public void map(LongWritable key, Text value, Context context )
throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split(",");


           if(str.length==12 ){
           disease.set(str[5]);

           String str1=str[10].replace("$", "");

           if(str1.matches("\\d+.+")){          //regularexpression to read degit and excluding decimal character
                   Float i=Float.parseFloat(str1);
           cost.set(i);
           }

              context.write(disease,cost);

           }

       }
    }

    public static class Reduce extends Reducer<Text, FloatWritable,
Text, DoubleWritable> {

       public void reduce(Text key, Iterable<FloatWritable> values,
Context context)
         throws IOException, InterruptedException {
           double sum = 0;
           for (FloatWritable val : values) {

               sum += val.get();
           }
           if(key.toString().equals("AL"))
           context.write(key, new DoubleWritable(sum));
       }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();

           @SuppressWarnings("deprecation")
                Job job = new Job(conf, "wordcount");
           job.setJarByClass(amountPerCity.class);
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(FloatWritable.class);

         job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(DoubleWritable.class);

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
