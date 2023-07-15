package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CountryCountByURL {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            /*String[] sa = value.toString().split(",");
            Text hostname = new Text();
            hostname.set(key); */
            context.write(key, value);
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split("\t");
            Text hostname = new Text();
            hostname.set(text[0]);
            Text val = new Text(text[1] + "\t" + text[2]);
            context.write(hostname, val);
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = "";
            ArrayList<String> others = new ArrayList<>();
            for (Text val : values) {
                if (val.toString().charAt(0) == '/'){
                    others.add(val.toString());
                } else {
                    country = val.toString();
                }
            }
            for (String v : others){
                context.write(new Text(country), new Text(v));
            }
        }
    }

}
