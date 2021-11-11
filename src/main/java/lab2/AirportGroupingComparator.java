package lab2;

import org.apache.hadoop.io.WritableComparable;

public class AirportGroupingComparator {
    public AirportGroupingComparator(){
        super(KeyWritableComparable.class);
    }

    @Override
    public int compare(WritableComparable o, WritableComparable o2){
        System.out.println("in compare");
        Movie m = (Movie)o;
        Movie m2 = (Movie)o2;
        System.out.println(m.compareTo(m2));
        return m.movieId.compareTo(m2.movieId);
    }
}
