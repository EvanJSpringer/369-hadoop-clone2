package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class HostCountByURL {

    public static final Class OUTPUT_KEY_CLASS = HostURLPair.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, HostURLPair, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(text[0]);
            HostURLPair newKey = new HostURLPair(text[0], text[6]);

            context.write(newKey, one);
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class ReducerImpl extends  Reducer<HostURLPair, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        @Override
        public void reduce(HostURLPair key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = values.iterator();

            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            String res = key.getHostname().toString() + "\t" + key.getURL().toString();
            context.write(new Text(res), result);
        }
    }

}
