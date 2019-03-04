package mapreduce;

import java.util.List;
import java.util.Map;

public interface MapReduceInterface {
	String[] split(String data);
	List<Word> map(String line);
	Map<String, List<Word>> shuffle(List<Word> words);
	List<Word> reduce();
	String execute();
}
