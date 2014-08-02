import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Compute {

	public static class Map extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		long bigramBGTotal;
		long bigramFGTotal;
		long bigramBGVocab;
		long bigramFGVocab;
		long unigramFGTotal;
		long unigramFGVocab;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			bigramBGTotal = conf.getLong(run_hadoop_phrase.BigramBGTotal, 0);
			bigramFGTotal = conf.getLong(run_hadoop_phrase.BigramFGTotal, 0);
			bigramBGVocab = conf.getLong(run_hadoop_phrase.BigramBGVocab, 0);
			bigramFGVocab = conf.getLong(run_hadoop_phrase.BigramFGVocab, 0);
			unigramFGTotal = conf.getLong(run_hadoop_phrase.UnigramFGTotal, 0);
			unigramFGVocab = conf.getLong(run_hadoop_phrase.UnigramFGVocab, 0);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			long fxy, bxy, fx, fy;
			fxy = bxy = fx = fy = 0;
			for (Text value : values) {
				String[] spans = value.toString().split(" ");
				fxy = Long.valueOf(spans[0]);
				bxy = Long.valueOf(spans[1]);
				fx += Long.valueOf(spans[2]);
				fy += Long.valueOf(spans[3]);
			}

			float pfxy = smoothedProb(fxy, bigramFGTotal, bigramFGVocab);
			float pbxy = smoothedProb(bxy, bigramBGTotal, bigramBGVocab);
			float pfx = smoothedProb(fx, unigramFGTotal, unigramFGVocab);
			float pfy = smoothedProb(fy, unigramFGTotal, unigramFGVocab);

			float phraseness = computeKLDivergence(pfxy, pfx * pfy);
			float informativeness = computeKLDivergence(pfxy, pbxy);
			float total = phraseness + informativeness;
			context.write(key, new Text(String.format("%f %f %f", total, phraseness, informativeness)));
		}

	}

	public static float smoothedProb(long count, long totalCount, long vocab) {
		return 1.0f * (count + 1) / (totalCount + vocab);
	}

	public static float computeKLDivergence(float p, float q) {
		return (float) (p * Math.log(1.0 * p / q));
	}
}
