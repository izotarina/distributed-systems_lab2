package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, KeyWritableComparable, String> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(",");

        if (key.get() != 0) {
            context.write(
                    new KeyWritableComparable(Integer.parseInt(columns[0].substring(1, columns[0].length() - 1)), 0),
                    new AirportListWritable(Integer.parseInt(columns[0].substring(1, columns[0].length() - 1)), columns[1].substring(1, columns[1].length() - 1)).getAirportName()
            );
        }
    }
}
