import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Message_Unigram {

	public static class Map extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String keyStr = key.toString();
			String[] spans = keyStr.split(" ");

			if (spans.length == 1) {
				// unigram
				context.write(key, value);
			} else {
				// bigram
				String x = spans[0];
				String y = spans[1];
				String valueX = String.format("%s %s %c", value.toString(), keyStr, 'x');
				String valueY = String.format("%s %s %c", value.toString(), keyStr, 'y');
				context.write(new Text(x), new Text(valueX));
				context.write(new Text(y), new Text(valueY));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long foreCount = 0;
			ArrayList<String> phrases = new ArrayList<String>();
			
			for (Text value : values) {
				String valueStr = value.toString();
				if (valueStr.split(" ").length == 1) {
					foreCount = Long.valueOf(valueStr);
				} else {
					phrases.add(valueStr);
				}
			}

			for (String phrase : phrases) {
				String[] spans = phrase.split(" ");
				String fxy = spans[0];
				String bxy = spans[1];
				String bigram = spans[2] + " " + spans[3];

				String fx, fy;
				if (spans[4].equals("x")) {
					fx = Long.toString(foreCount);
					fy = "0";
				} else {
					fy = Long.toString(foreCount);
					fx = "0";
				}

				String value = String.format("%s %s %s %s", fxy, bxy, fx, fy);
				context.write(new Text(bigram), new Text(value));
			}
		}
	}

}
