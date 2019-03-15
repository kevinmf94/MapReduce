package mapreduce;

/**
 * Class represents a word with the number of repeats
 */
public class Word {
	private String word;
	private int count;
	
	public Word(String word, int count) {
		super();
		this.word = word;
		this.count = count;
	}
	
	public String getWord() {
		return word;
	}
	
	public int getCount() {
		return count;
	}
	
	@Override
	public String toString() {
		return String.format("(%s:%d)", word, count);
	}
}
