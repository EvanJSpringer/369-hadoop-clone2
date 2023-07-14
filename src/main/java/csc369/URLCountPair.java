package csc369;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class URLCountPair
        implements Writable, WritableComparable<URLCountPair> {

    private final Text url = new Text();
    private final Text count = new Text();

    public URLCountPair() {
    }

    public URLCountPair(String host, String url) {
        this.url.set(url);
        this.count.set(host);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        url.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(URLCountPair pair) {
        if (url.compareTo(pair.getURL()) == 0) {
            return count.compareTo(pair.count);
        }
        return url.compareTo(pair.getURL());
    }

    public Text getCount() {
        return count;
    }
    public Text getURL() {
        return url;
    }
}