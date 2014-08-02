import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class run_hadoop_phrase {
	static enum GlobalCounters {
		BigramFGTotal, BigramBGTotal, BigramFGVocab, BigramBGVocab, UnigramFGTotal, UnigramFGVocab
	}

	static String BigramFGTotal = "BigramFGTotal";
	static String BigramBGTotal = "BigramBGTotal";
	static String BigramFGVocab = "BigramFGVocab";
	static String BigramBGVocab = "BigramBGVocab";
	static String UnigramFGTotal = "UnigramFGTotal";
	static String UnigramFGVocab = "UnigramFGVocab";

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Path unigramInputPath = new Path(args[0]);
		Path bigramInputPath = new Path(args[1]);
		Path aggregateOutputPath = new Path(args[2]);
		Path unigramMessagePath = new Path(args[4]);
		Path finalResultPath = new Path(args[5]);

		/* job1: aggregate the count, also record total count information */
		Configuration aggConf = new Configuration();
		aggConf.set("mapred.textoutputformat.separator", "\t");
		Job aggJob = new Job(aggConf, "Aggregate");

		aggJob.setJarByClass(run_hadoop_phrase.class);
		aggJob.setMapperClass(Aggregate.Map.class);
		aggJob.setReducerClass(Aggregate.Reduce.class);

		aggJob.setInputFormatClass(TextInputFormat.class);
		aggJob.setOutputFormatClass(TextOutputFormat.class);

		aggJob.setMapOutputKeyClass(Text.class);
		aggJob.setMapOutputValueClass(Text.class);
		aggJob.setOutputKeyClass(Text.class);
		aggJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(aggJob, unigramInputPath);
		FileInputFormat.addInputPath(aggJob, bigramInputPath);
		FileOutputFormat.setOutputPath(aggJob, aggregateOutputPath);

		aggJob.waitForCompletion(true);

		Configuration computeConf = new Configuration();
		computeConf.setLong(BigramBGTotal, aggJob.getCounters().findCounter(GlobalCounters.BigramBGTotal).getValue());
		computeConf.setLong(BigramBGVocab, aggJob.getCounters().findCounter(GlobalCounters.BigramBGVocab).getValue());
		computeConf.setLong(BigramFGTotal, aggJob.getCounters().findCounter(GlobalCounters.BigramFGTotal).getValue());
		computeConf.setLong(BigramFGVocab, aggJob.getCounters().findCounter(GlobalCounters.BigramFGVocab).getValue());
		computeConf.setLong(UnigramFGTotal, aggJob.getCounters().findCounter(GlobalCounters.UnigramFGTotal).getValue());
		computeConf.setLong(UnigramFGVocab, aggJob.getCounters().findCounter(GlobalCounters.UnigramFGVocab).getValue());

		/* job2: message uniform job */
		Configuration msgUniformConf = new Configuration();
		Job msgUniformJob = new Job(msgUniformConf, "MessageUniform");
		msgUniformJob.setJarByClass(run_hadoop_phrase.class);

		msgUniformJob.setJarByClass(run_hadoop_phrase.class);
		msgUniformJob.setMapperClass(Message_Unigram.Map.class);
		msgUniformJob.setReducerClass(Message_Unigram.Reduce.class);

		msgUniformJob.setInputFormatClass(KeyValueTextInputFormat.class);
		msgUniformJob.setOutputFormatClass(TextOutputFormat.class);
		// msgUniformConf.set("key.value.separator.in.input.line", "\t");
		msgUniformConf.set("mapred.textoutputformat.separator", "\t");

		msgUniformJob.setMapOutputKeyClass(Text.class);
		msgUniformJob.setMapOutputValueClass(Text.class);
		msgUniformJob.setOutputKeyClass(Text.class);
		msgUniformJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(msgUniformJob, aggregateOutputPath);
		FileOutputFormat.setOutputPath(msgUniformJob, unigramMessagePath);

		msgUniformJob.waitForCompletion(true);

		/* job3: compute job */
		Job computeJob = new Job(computeConf, "Compute");

		computeJob.setJarByClass(run_hadoop_phrase.class);

		computeJob.setMapperClass(Compute.Map.class);
		computeJob.setReducerClass(Compute.Reduce.class);

		computeJob.setInputFormatClass(KeyValueTextInputFormat.class);
		computeJob.setOutputFormatClass(TextOutputFormat.class);
		// computeConf.set("key.value.separator.in.input.line", "\t");
		computeConf.set("mapred.textoutputformat.separator", "\t");

		computeJob.setMapOutputKeyClass(Text.class);
		computeJob.setMapOutputValueClass(Text.class);
		computeJob.setOutputKeyClass(Text.class);
		computeJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(computeJob, unigramMessagePath);
		FileOutputFormat.setOutputPath(computeJob, finalResultPath);

		computeJob.waitForCompletion(true);
	}
}
