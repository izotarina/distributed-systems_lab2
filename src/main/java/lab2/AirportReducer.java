package lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportReducer extends Reducer<KeyWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(KeyWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String airportName = iterator.next();
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        context.write(key, new LongWritable(count));
    }
}