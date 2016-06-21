package com.wikipedia;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "App");

        job.setJarByClass(App.class);
        //Map Reduce 1
        job.setMapperClass(WikipediaToItemPrefsMapper.class);
        job.setReducerClass(WikipediaToUserVectorReducer.class);
        job.setMapOutputKeyClass(VarLongWritable.class);
        job.setMapOutputValueClass(VarLongWritable.class);
        job.setOutputKeyClass(VarLongWritable.class);
        job.setOutputValueClass(VectorWritable.class);
        //Map Reduce 2
        Job job2= new Job(conf, "App");
        job2.setMapperClass(UserVectorToCooccurrenceMapper.class);
        job2.setReducerClass(UserVectorToCooccurrenceReducer.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(VectorWritable.class);
        job.add
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
