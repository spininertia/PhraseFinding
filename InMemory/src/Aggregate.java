import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

/**
 * aggregate statsitics for foreground corpus and backgorund corpus
 * 
 * @author Siping Ji <sipingji@cmu.edu>
 * 
 */
public class Aggregate {

	private static final long THRESHOLD = 1989;
	private long fore_vocab_size = 0;
	private long back_vocab_size = 0;
	private long fore_total_count = 0;
	private long back_total_count = 0;

	private BufferedReader reader = new BufferedReader(new InputStreamReader(
			System.in));
	private BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
			System.out));

	/**
	 * when mode == 1, aggregate bi-gram when mode == 1, aggregate uni-gram
	 * 
	 * @param mode
	 * @throws IOException
	 */
	public void aggregate(long mode) throws IOException {
		String line;
		String prev = null;
		long prevForeCount = 0;
		long prevBackCount = 0;
		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split("\t");
			String gram = tokens[0];
			long decade = Long.valueOf(tokens[1]);
			long count = Long.valueOf(tokens[2]);

			if (!gram.equals(prev)) {
				yield(prev, prevForeCount, prevBackCount);
				recordCorpusStat(prevForeCount, prevBackCount);
				prev = gram;
				if (decade > THRESHOLD) {
					prevForeCount = count;
					prevBackCount = 0;
				} else {
					prevForeCount = 0;
					prevBackCount = count;
				}

			} else {
				if (decade > THRESHOLD) {
					prevForeCount += count;
				} else {
					prevBackCount += count;
				}
			}
		}
		recordCorpusStat(prevForeCount, prevBackCount);
		yield(prev, prevForeCount, prevBackCount);
		yield(String.format("%s%d", Utils.VOCAB_SIGN, mode + 1), fore_vocab_size,
				back_vocab_size);
		yield(String.format("%s%d", Utils.COUNT_SIGN, mode + 1), fore_total_count,
				back_total_count);
		writer.flush();
	}
	/**
	 * the format for unigram counts are as follows unigram \t
	 * foregroud_count,background_count
	 * 
	 * @param gram
	 * @param foreGroundCount
	 * @param backGroundCount
	 * @throws IOException
	 */
	private final void yield(String gram, long foreGroundCount,
			long backGroundCount) throws IOException {
		if (gram != null) {
//			writer.write(String.format("%s\t%d,%d\n", gram, foreGroundCount,
//					backGroundCount));
			writer.write(gram + "\t" + foreGroundCount + "," + backGroundCount + "\n");
		}
	}

	private final void recordCorpusStat(long foreCount, long backCount) {
		if (foreCount > 0) {
			fore_vocab_size++;
			fore_total_count += foreCount;
		}

		if (backCount > 0) {
			back_vocab_size++;
			back_total_count += backCount;
		}
	}

	public static void main(String[] args) throws NumberFormatException,
			IOException {
		Aggregate aggregator = new Aggregate();
		aggregator.aggregate(Long.valueOf(args[0]));
	}
}
