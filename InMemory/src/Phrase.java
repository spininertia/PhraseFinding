
public class Phrase implements Comparable<Phrase>{
	String phrase;
	float phraseness;
	float informativeness;
	
	public Phrase(String key) {
		phrase = key;
	}
	
	@Override
	public String toString() {
		return String.format("%s\t%f\t%f\t%f", phrase, phraseness + informativeness, phraseness, informativeness);
	}
	
	public float getTotalScore() {
		return phraseness + informativeness;
	}

	@Override
	public int compareTo(Phrase p1) {
		float sub = p1.getTotalScore() - this.getTotalScore();
		if (sub > 0)
			return -1;
		else if (sub < 0)
			return 1;
		return 0;
	}
}
