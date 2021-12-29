package com.ramesh.passenger;
 

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PassengerRushAnalysisDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(),
                new PassengerRushAnalysisDriver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

		args = new String[] { 
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/5.CustomWritableComparable/Input_Data/passengerRush.txt",
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/5.CustomWritableComparable/Output_Data/"};
				 
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[1])); 
				 
				if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
				
				System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
				
 
        Path input=new Path(args[0]);
        Path output=new Path(args[1]);
        Configuration conf=new Configuration();

        /*
        * UnComment below three lines to enable local debugging of map reduce job
         * */
        /*conf.set("fs.defaultFS", "local");
        conf.set("mapreduce.job.maps","1");
        conf.set("mapreduce.job.reduces","1");
        */

        Job job=Job.getInstance(conf,"Hadoop Custom Key Writable Example");
        job.setJarByClass(PassengerRushAnalysisDriver.class);
        job.setMapperClass(PassengerRushMapper.class);
        job.setReducerClass(PassengerRushReducer.class);
        job.setMapOutputKeyClass(GeoLocationWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(GeoLocationWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setSpeculativeExecution(false);
        boolean success = job.waitForCompletion(true);
        return (success?0:1);
    }
}