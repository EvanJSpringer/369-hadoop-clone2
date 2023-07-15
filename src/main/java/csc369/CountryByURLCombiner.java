package csc369;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class CountryByURLCombiner {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split("\t");
            Text url = new Text();
            url.set(sa[0]);
            Text country = new Text();
            country.set(sa[1]);
            context.write(url, country);
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            Text country = (Text) wc1;
            Text country2 = (Text) wc2;
            return country.compareTo(country2);
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text word, Iterable<Text> countries,
                              Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = countries.iterator();
            ArrayList<String> vals = new ArrayList<>();
            for (Text country : countries){
                if (vals.size() == 0 || !(country.toString().equals(vals.get(vals.size() - 1)))){
                    vals.add(country.toString());
                }
            }
            StringBuilder countryList = new StringBuilder();
            for (String val : vals){
                countryList.append(", ").append(val);
            }
            context.write(word, new Text(countryList.toString()));
        }
    }

}
