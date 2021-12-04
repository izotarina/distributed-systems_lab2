package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, KeyWritableComparable, Text> {
    private final static int AIRPORT_CODE_COLUMN_INDEX = 0;
    private final static int AIRPORT_NAME_COLUMN_INDEX = 1;
    private final static String DELIMITER = ",";
    private final static int AIRPORT_DATA_FLAG = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputLine = value.toString();
        String[] columns = inputLine.split(DELIMITER);

        if (key.get() != 0) {
            int airportCode = Integer.parseInt(getStringWithoutQuotes(columns[AIRPORT_CODE_COLUMN_INDEX]));
            String airportName = getStringWithoutQuotes(columns[AIRPORT_NAME_COLUMN_INDEX]);

            context.write(
                    new KeyWritableComparable(airportCode, AIRPORT_DATA_FLAG),
                    new Text(new AirportListWritable(airportCode, airportName).getAirportName())
            );
        }
    }

    private static String getStringWithoutQuotes(String string) {
        return string.substring(1, string.length() - 1);
    }
}
