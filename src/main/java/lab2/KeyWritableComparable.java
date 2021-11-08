package lab2;

public class KeyWritableComparable {
    private Text siteURL, reqDate, timestamp, ipaddress;
    private IntWritable reqNo;

    //Default Constructor
    public KeyWritableComparable()
    {
        this.siteURL = new Text();
        this.reqDate = new Text();
        this.timestamp = new Text();
        this.ipaddress = new Text();
        this.reqNo = new IntWritable();
    }

    //to get IP address from WebLog Record
    public Text getIp()
    {
        return ipaddress;
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in) throws IOException
    {
        ipaddress.readFields(in);
        timestamp.readFields(in);
        reqDate.readFields(in);
        reqNo.readFields(in);
        siteURL.readFields(in);
    }

    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException
    {
        ipaddress.write(out);
        timestamp.write(out);
        reqDate.write(out);
        reqNo.write(out);
        siteURL.write(out);
    }

    @Override
    public int compareTo(WebLogWritable o)
    {
        if (ipaddress.compareTo(o.ipaddress)==0)
        {
            return (timestamp.compareTo(o.timestamp));
        }
        else return (ipaddress.compareTo(o.ipaddress));
    }

    @Override
    public boolean equals(Object o)
    {Rules for creating custom Hadoop Writable
        if (o instanceof KeyWritableComparable)
        {
            KeyWritableComparable other = (KeyWritableComparable) o;
            return ipaddress.equals(other.ipaddress) && timestamp.equals(other.timestamp);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return ipaddress.hashCode();
    }
}
