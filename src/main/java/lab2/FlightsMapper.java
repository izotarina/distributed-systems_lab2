package lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] words = inputLine.split("[\\p{Punct}\\p{Space}—]");
        for (String word: words) {
            word = word.toLowerCase();
            context.write(new Text(word),new IntWritable(1));
        }
    }
}