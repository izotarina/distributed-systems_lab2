package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, KeyWritableComparable, Text> {
    private final static int AIRPORT_COLUMN_INDEX = 14;
    private final static int DELAY_COLUMN_INDEX = 18;
    private final static int IS_CANCELLED_COLUMN_INDEX = 19;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(",");

        if (key.get() != 0) {
            float delayTime = new AirportFlightsWritable(Integer.parseInt(columns[AIRPORT_COLUMN_INDEX]),
                    columns[IS_CANCELLED_COLUMN_INDEX].length() > 0 ? Float.parseFloat(columns[IS_CANCELLED_COLUMN_INDEX]) : 0,
                    columns[DELAY_COLUMN_INDEX].length() > 0 ? Float.parseFloat(columns[DELAY_COLUMN_INDEX]) : 0).getDelayTime();

            if (delayTime > 0) {
                context.write(
                        new KeyWritableComparable(Integer.parseInt(columns[AIRPORT_COLUMN_INDEX]), 1),
                        new Text(String.valueOf(delayTime))
                );
            }
        }
    }
}
