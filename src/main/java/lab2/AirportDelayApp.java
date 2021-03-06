package lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AirportDelayApp {
    public static void main(String[] args) throws Exception {
        /*Provides access to configuration parameters*/
        Configuration conf = new Configuration();
        /*Creating Filesystem object with the configuration*/
        FileSystem fs = FileSystem.get(conf);
        /*Check if output path (args[2])exist or not*/
        if(fs.exists(new Path(args[2]))){
            /*If exist delete the output path*/
            fs.delete(new Path(args[2]),true);
        }

        if (args.length != 3) {
            System.err.println("Usage: lab2.AirportDelayApp <input1 path> <input2 path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(AirportDelayApp.class);
        job.setJobName("Airport delay");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirportMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, FlightsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(AirportGroupingComparator.class);
        job.setReducerClass(AirportReducer.class);
        job.setMapOutputKeyClass(KeyWritableComparable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}