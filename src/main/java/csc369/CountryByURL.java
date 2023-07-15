package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class CountryByURL {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value,
                           Context context) throws IOException, InterruptedException {
            /*String[] sa = value.toString().split(",");
            Text hostname = new Text();
            hostname.set(key);
            Text country = new Text();
            country.set(sa[0]);*/
            context.write(key, value);
        }
    }

    public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String text[] = value.toString().split(" ");
            Text hostname = new Text();
            hostname.set(text[0]);
            Text url = new Text();
            url.set(text[6]);
            context.write(hostname, url);
        }
    }

    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends  Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
            String country = "";
            ArrayList<String> urls = new ArrayList<>();
            for (Text val : values) {
                if (val.toString().charAt(0) != '/'){
                    country = val.toString();
                } else {
                    urls.add(val.toString());
                }
            }
            for (String url : urls){
                context.write(new Text(url), new Text(country));
            }
        }
    }

}
