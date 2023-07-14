package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class HostURLPair
        implements Writable, WritableComparable<HostURLPair> {

    private final Text host = new Text();
    private final Text url = new Text();

    public HostURLPair() {
    }

    public HostURLPair(String host, String url) {
        this.url.set(url);
        this.host.set(host);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        host.write(out);
        url.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host.readFields(in);
        url.readFields(in);
    }

    @Override
    public int compareTo(HostURLPair pair) {
        if (host.compareTo(pair.getHostname()) == 0) {
            return url.compareTo(pair.url);
        }
        return host.compareTo(pair.getHostname());
    }

    public Text getHostname() {
        return host;
    }
    public Text getURL() {
        return url;
    }
}