package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class urlcountPair
        implements Writable, WritableComparable<urlcountPair> {

    private final Text url = new Text();
    private final Text count = new Text();

    public urlcountPair() {
    }

    public urlcountPair(String url, String count) {
        this.url.set(url);
        this.count.set(count);
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
    public int compareTo(urlcountPair pair) {
        if (url.compareTo(pair.getURL()) == 0) {
            return count.compareTo(pair.count);
        }
        return url.compareTo(pair.getURL());
    }

    public Text getURL() {
        return url;
    }

}