package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, KeyWritableComparable, String> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(",");

        if (key.get() != 0) {
            context.write(
                    new KeyWritableComparable(Integer.parseInt(columns[0]), 0),
                    new AirportListWritable(Integer.parseInt(columns[0]), columns[1]).getAirportName()
            );
        }
    }
}
