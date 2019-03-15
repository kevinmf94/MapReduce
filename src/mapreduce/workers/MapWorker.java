package mapreduce.workers;

import java.util.List;
import java.util.concurrent.Callable;

import mapreduce.Word;

public abstract class MapWorker implements Callable<List<Word>> {

	private List<String> lines;
	
	public MapWorker() {}
	
	public void setLines(List<String> lines) {
		this.lines = lines;
	}
	
	public List<String> getLines() {
		return lines;
	}
}
