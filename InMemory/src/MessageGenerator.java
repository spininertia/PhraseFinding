import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;


public class MessageGenerator {
	private static BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	private static BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
	
	public static void generateMessage() throws IOException {
		String line;
		while ((line = reader.readLine()) != null) {
			String bigram = line.split("\t")[0];
			if (!(bigram.startsWith(Utils.COUNT_SIGN) || bigram.startsWith(Utils.VOCAB_SIGN))) {
				String[] words = bigram.split(" ");
				yield(words[0], bigram);
				yield(words[1], bigram);
			}
		}
		writer.flush();
	}
	
	private final static void yield(String word, String phrase) throws IOException {
//		writer.write(String.format("%s\t~%s\n", word, phrase));
		writer.write(word + "\t~" + phrase + "\n");
	}
	
	public static void main(String[] args) throws IOException {
		MessageGenerator.generateMessage();
	}
}
