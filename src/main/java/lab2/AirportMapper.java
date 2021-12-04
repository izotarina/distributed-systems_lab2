package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, KeyWritableComparable, Text> {
    private final static int AIRPORT_CODE_COLUMN_INDEX = 0;
    private final static int AIRPORT_NAME_COLUMN_INDEX = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(",");

        if (key.get() != 0) {
            int airportCode = Integer.parseInt(columns[AIRPORT_CODE_COLUMN_INDEX].substring(1, columns[AIRPORT_CODE_COLUMN_INDEX].length() - 1));
            String airportName = columns[AIRPORT_NAME_COLUMN_INDEX].substring(1, columns[AIRPORT_NAME_COLUMN_INDEX].length() - 1);

            context.write(
                    new KeyWritableComparable(airportCode, 0),
                    new Text(airportName)
            );
        }
    }
}
