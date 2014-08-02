import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.PriorityQueue;

public class PhraseGenerator {

	private static final int K = 20;

	private BufferedReader reader = new BufferedReader(new InputStreamReader(
			System.in));
	private CorpusStatistics foreground = new CorpusStatistics();
	private CorpusStatistics background = new CorpusStatistics();

	private PriorityQueue<Phrase> queue = new PriorityQueue<Phrase>();

	public void generatePhrase() throws IOException {
		String line;
		String prevKey = null;
		long foreXY, backXY, foreX, foreY, backX, backY;
		foreXY = backXY = foreX = foreY = backX = backY = 0;

		while ((line = reader.readLine()) != null) {
			String[] tokens = line.split("\t");
			String key = tokens[0];
			String[] counts = tokens[1].split(",");

			// record corpus statistics
			if (key.startsWith(Utils.VOCAB_SIGN)) {
				if (isUniInfo(key)) {
					foreground.uniGramVocabSize = Long.valueOf(counts[0]);
					background.uniGramVocabSize = Long.valueOf(counts[1]);
				} else {
					foreground.biGramVocabSize = Long.valueOf(counts[0]);
					background.biGramVocabSize = Long.valueOf(counts[1]);
				}
			} else if (key.startsWith(Utils.COUNT_SIGN)) {
				if (isUniInfo(key)) {
					foreground.uniGramTotalCount = Long.valueOf(counts[0]);
					background.uniGramTotalCount = Long.valueOf(counts[1]);
				} else {
					foreground.biGramTotalCount = Long.valueOf(counts[0]);
					background.biGramTotalCount = Long.valueOf(counts[1]);
				}
			} else {
				if (!key.equals(prevKey)) {
					// phrase count
					if (prevKey != null) {
						Phrase phrase = constructPhrase(prevKey, foreXY,
								backXY, foreX, foreY, backX, backY);
						enque(phrase);
					}

					prevKey = key;
					foreXY = Long.valueOf(counts[0]);
					backXY = Long.valueOf(counts[1]);
				} else {
					// unigram count
					long fore = Long.valueOf(counts[1]);
					long back = Long.valueOf(counts[2]);

					if ("x".equals(counts[0])) {
						foreX = fore;
						backX = back;
					} else {
						foreY = fore;
						backY = back;
					}
				}
			}
		}
		Phrase phrase = constructPhrase(prevKey, foreXY, backXY, foreX, foreY,
				backX, backY);
		enque(phrase);
		printResult();
	}

	private final void printResult() {
		Phrase phrase = queue.poll();
		if (!queue.isEmpty()) {
			printResult();
		}
		System.out.println(phrase);
	}
	
	private void enque(Phrase phrase) {
		if (queue.size() < K) {
			queue.add(phrase);
		} else {
			if (phrase.getTotalScore() > queue.peek().getTotalScore()) {
				queue.poll();
				queue.add(phrase);
			}
		}
	}

	private Phrase constructPhrase(String key, long foreXY, long backXY,
			long foreX, long foreY, long backX, long backY) {
		Phrase phrase = new Phrase(key);

		// compute informativeness
		float p_fg_xy = computeBigramProb(foreXY, foreground);
		float p_bg_xy = computeBigramProb(backXY, background);
		float informativeness = computeKLDivergence(p_fg_xy, p_bg_xy);

		// compute phraseness
		float p_x = computeUnigramProb(foreX, foreground);
		float p_y = computeUnigramProb(foreY, foreground);
		float phraseness = computeKLDivergence(p_fg_xy, p_x * p_y);

		phrase.informativeness = informativeness;
		phrase.phraseness = phraseness;

		return phrase;
	}

	private final float computeUnigramProb(long count, CorpusStatistics corpus) {
		return 1.0f * (count + 1)
				/ (corpus.uniGramTotalCount + corpus.uniGramVocabSize);
	}

	private final float computeBigramProb(long count, CorpusStatistics corpus) {
		return 1.0f * (count + 1)
				/ (corpus.biGramTotalCount + corpus.biGramVocabSize);
	}

	private final float computeKLDivergence(float p, float q) {
		return (float) (p * Math.log(1.0 * p / q));
	}

	private final boolean isUniInfo(String key) {
		return key.charAt(1) == '1';
	}

	public static void main(String args[]) throws IOException {
		PhraseGenerator phraseGenerator = new PhraseGenerator();
		phraseGenerator.generatePhrase();
	}
}
