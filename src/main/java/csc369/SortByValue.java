package csc369;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SortByValue {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    IntWritable valueText = new IntWritable(-1 * Integer.parseInt(sa[1]));
        Text keyText = new Text();
        keyText.set(sa[0]);
	    context.write(valueText, keyText);
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
	private Text result = new Text();
    
        @Override
	protected void reduce(IntWritable value, Iterable<Text> key,
			      Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = key.iterator();
            while (itr.hasNext()){
                result.set(itr.next());
                context.write(new IntWritable(value.get() * -1), result);
            }
       }
    }

}
