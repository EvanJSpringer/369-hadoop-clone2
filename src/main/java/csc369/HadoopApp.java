package csc369;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCount".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountryCount.CountryMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountryCount.LogMapper.class );

		job.setReducerClass(CountryCount.JoinReducer.class);

		job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountryCountCombine".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CountryCountCombine.ReducerImpl.class);
		job.setMapperClass(CountryCountCombine.MapperImpl.class);
		job.setOutputKeyClass(CountryCountCombine.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryCountCombine.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("SortByValue".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(SortByValue.ReducerImpl.class);
		job.setMapperClass(SortByValue.MapperImpl.class);
		job.setOutputKeyClass(SortByValue.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(SortByValue.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("HostCountByURL".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(HostCountByURL.ReducerImpl.class);
		job.setMapperClass(HostCountByURL.MapperImpl.class);
		job.setOutputKeyClass(HostCountByURL.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(HostCountByURL.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("CountryCountByURL".equalsIgnoreCase(otherArgs[0])) {
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				KeyValueTextInputFormat.class, CountryCountByURL.CountryMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
				TextInputFormat.class, CountryCountByURL.LogMapper.class );

		job.setReducerClass(CountryCountByURL.JoinReducer.class);

		job.setOutputKeyClass(CountryCountByURL.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryCountByURL.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
	} else if ("CountryCountSort".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(CountryCountSort.ReducerImpl.class);
		job.setMapperClass(CountryCountSort.MapperImpl.class);
		job.setOutputKeyClass(CountryCountSort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(CountryCountSort.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
