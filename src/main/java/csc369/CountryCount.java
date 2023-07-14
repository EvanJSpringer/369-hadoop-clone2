package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class CountryCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(",");
            Text hostname = new Text();
            hostname.set(sa[0]);
            Text country = new Text();
            country.set("country");
            context.write(hostname, country);
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(text[0]);
            Text count = new Text();
            count.set("1");
            context.write(hostname, count);
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            Text country = new Text();
            //IntWritable count;
            int sum = 0;
            for (Text val : values) {
                if (Character.isDigit(val.toString().charAt(0))){
                    sum += Integer.parseInt(val.toString());
                } else {
                    country = val;
                }
            }
            context.write(country, new IntWritable(sum));
        }
    }

}
