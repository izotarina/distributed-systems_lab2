package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<KeyWritableComparable, Text> {
    @Override
    public int getPartition(KeyWritableComparable key, Text text, int countPartitions) {
        return key.getAirportID() % countPartitions;
    }
}
