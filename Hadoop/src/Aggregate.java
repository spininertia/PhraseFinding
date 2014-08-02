import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Aggregate {

	public static final int fgCutOff = 1970;
	public static Set<String> stopWords = new HashSet<String>();
	static {
		String stopwords = "i,the,to,and,a,an,of,it,you,that,in,my,is,was,for";
		for (String word : stopwords.split(",")) {
			stopWords.add(word);
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] spans = line.split("\t");
			String text = spans[0];
			if (!containStopWords(text)) {
				String val = spans[1] + "\t" + spans[2];
				context.write(new Text(text), new Text(val));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long foreCount = 0;
			long backCount = 0;

			for (Text value : values) {
				String[] spans = value.toString().split("\t");
				int decade = Integer.valueOf(spans[0]);
				long count = Long.valueOf(spans[1]);
				if (decade < fgCutOff) {
					foreCount += count;
				} else {
					backCount += count;
				}
			}

			Text value = new Text();

			if (isBigram(key.toString())) {
				context.getCounter(run_hadoop_phrase.GlobalCounters.BigramBGTotal).increment(backCount);

				if (backCount > 0) {
					context.getCounter(run_hadoop_phrase.GlobalCounters.BigramBGVocab).increment(1);
				}

				context.getCounter(run_hadoop_phrase.GlobalCounters.BigramFGTotal).increment(foreCount);

				if (foreCount > 0) {
					context.getCounter(run_hadoop_phrase.GlobalCounters.BigramFGVocab).increment(1);
				}

				value.set(String.format("%d %d", foreCount, backCount));
			} else {
				context.getCounter(run_hadoop_phrase.GlobalCounters.UnigramFGTotal).increment(foreCount);
				if (foreCount > 0) {
					context.getCounter(run_hadoop_phrase.GlobalCounters.UnigramFGVocab).increment(1);
				}
				value.set(String.format("%d", foreCount));
			}

			context.write(key, value);
		}

	}

	public static boolean isBigram(String text) {
		return text.split(" ").length == 2;
	}

	public static boolean containStopWords(String text) {
		for (String word : text.split(" ")) {
			if (stopWords.contains(word)) {
				return true;
			}
		}
		return false;
	}
}
