package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CountryCountSort {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCountPair, Text> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split("\t");
        /*IntWritable valueText = new IntWritable(-1 * Integer.parseInt(sa[1]));
        Text keyText = new Text();
        keyText.set(sa[0]); */
            CountryCountPair pairKey = new CountryCountPair(sa[0], Integer.parseInt(sa[2]));
            context.write(pairKey, value);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, Text, IntWritable> {
        private Text result = new Text();

        protected void reduce(Text value, Iterable<CountryCountPair> key,
                              Context context) throws IOException, InterruptedException {
            Iterator<CountryCountPair> itr = key.iterator();
            String[] sa = value.toString().split("\t");
            while (itr.hasNext()){
                //result.set(itr.next());
                Text res = new Text(sa[0] + sa[1]);
                context.write(res, itr.next().getCount());
            }
       }
    }

}
