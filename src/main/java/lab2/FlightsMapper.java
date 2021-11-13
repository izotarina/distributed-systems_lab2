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
                    new KeyWritableComparable(Integer.parseInt(columns[14].substring(1, columns[14].length() - 1)), 1),
                    new Text(String.valueOf(new AirportFlightsWritable(Integer.parseInt(columns[14].substring(1, columns[14].length() - 1)),
                            Integer.parseInt(columns[19].substring(1, columns[19].length() - 1)), Integer.parseInt(columns[18].substring(1, columns[18].length() - 1))).getDelayTime()))
            );
        }
    }
}
