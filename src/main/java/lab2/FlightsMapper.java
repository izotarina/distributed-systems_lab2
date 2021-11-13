package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, KeyWritableComparable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(",");

        if (key.get() != 0) {
            context.write(
                    new KeyWritableComparable(Integer.parseInt(columns[14]), 1),
                    new Text(String.valueOf(new AirportFlightsWritable(Integer.parseInt(columns[14]), Float.parseFloat(columns[19]), Float.parseFloat(columns[18])).getDelayTime()))
            );
        }
    }
}
