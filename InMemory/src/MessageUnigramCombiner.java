import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class MessageUnigramCombiner {
	private static BufferedReader reader = new BufferedReader(
			new InputStreamReader(System.in));
	private static BufferedWriter writer = new BufferedWriter(
			new OutputStreamWriter(System.out));

	public static void combine() throws IOException {
		String prevKey = null;
		long foreCount = 0;
		long backCount = 0;

		String line;
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split("\t");
			String key = tokens[0];
			if (key.startsWith(Utils.COUNT_SIGN) || key.startsWith(Utils.VOCAB_SIGN)) {
				writer.write(line + "\n");
			} else {
				if (!key.equals(prevKey)) {
					String[] counts = tokens[1].split(",");
					foreCount = Long.valueOf(counts[0]);
					backCount = Long.valueOf(counts[1]);
					prevKey = key;
				} else {
					String phrase = tokens[1].substring(1);
					String[] unigrams = phrase.split(" ");
					String x = unigrams[0];
					char position;
					if (key.equals(x)) {
						position = 'x';
					} else {
						position = 'y';
					}
					yield(phrase, position, foreCount, backCount);
				}
			}
		}
		writer.flush();
	}

	private final static void yield(String phrase, char position,
			long foreCount, long backCount) throws IOException {
//		writer.write(String.format("%s\t%c,%d,%d\n", phrase, position,
//				foreCount, backCount));
		writer.write(phrase + "\t" + position + "," + foreCount + "," + backCount + "\n");
	}

	public static void main(String args[]) throws IOException {
		MessageUnigramCombiner.combine();
	}
}
