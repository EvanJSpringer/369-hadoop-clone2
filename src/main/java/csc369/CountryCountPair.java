package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryCountPair
        implements Writable, WritableComparable<CountryCountPair> {

    private final Text host = new Text();
    private final IntWritable count = new IntWritable();

    public CountryCountPair() {
    }

    public CountryCountPair(String host, int count) {
        this.count.set(count);
        this.host.set(host);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        host.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(CountryCountPair pair) {
        if (host.compareTo(pair.getHostname()) == 0) {
            return -1 * count.compareTo(pair.count);
        }
        return host.compareTo(pair.getHostname());
    }

    public Text getHostname() {
        return host;
    }
    public IntWritable getCount() {
        return count;
    }
}