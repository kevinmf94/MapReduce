//package mapreduce;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.TreeMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class MapReduce implements MapReduceInterface {
//	
//	private static final int NTHREADS = 4; 
//	
//	private List<String> files;
//	private TreeMap<String, List<Word>> shuffle = new TreeMap<>();
//	private List<Word> reduced = new ArrayList<Word>();
//	
//	public MapReduce(List<String> files) {
//		this.files = files;
//	}
//	
//	public String readFile(String fileName) {
//		
//		byte[] data = null;
//		
//		try {
//			File file = new File(fileName);
//			FileInputStream fis = new FileInputStream(file);
//			data = new byte[(int) file.length()];
//			fis.read(data);
//			fis.close();
//		} catch (IOException e) {
//			System.out.println("Error de lectura");
//			e.printStackTrace();
//		}
//
//		return new String(data);
//	}
//	
//	@Override
//	public String[] split(String data) {
//		return data.split("\n");
//	}
//
//	@Override
//	public List<Word> map(String line) {
//		return this.reduced;
//	}
//
//	@Override
//	public Map<String, List<Word>> shuffle(List<Word> words) {
//		
//		for(Word word : words) {
//			if(!shuffle.containsKey(word.getWord())) {
//				shuffle.put(word.getWord(), new ArrayList<Word>());
//			}
//			
//			shuffle.get(word.getWord()).add(word);
//		}
//		
//		return shuffle;
//	}
//
//	@Override
//	public List<Word> reduce() {
//		List<Word> reduced = new ArrayList<Word>();
//		
//		for(String key : shuffle.keySet()) {
//			reduced.add(new Word(key, shuffle.get(key).size()));
//		}
//		
//		return reduced;
//	}
//
//	public String execute() {
//		String result = "";
//		String data = "";
//		ExecutorService pool = Executors.newFixedThreadPool(NTHREADS);  
//		
//		for(String file : files) {
//			result += file+":\n";
//			
//			data = readFile(file);
//			String[] splitted = split(data);
//
//			for(String line : splitted) {
//				pool.execute(new MapWorker(line));
//			}
//
//
//			pool.shutdown();
//			while (!pool.isTerminated()) {
//	        }
//			//System.out.println(shuffle);
//			
//			pool = Executors.newFixedThreadPool(NTHREADS);  
//			for(String word : shuffle.keySet()) {
//				pool.execute(new ReduceWorker(shuffle.get(word)));
//			}
//			
//			pool.shutdown();
//			while (!pool.isTerminated()) {
//	        }
//			
//			for(Word word : reduced) {
//				result += word.toString();
//			}
//			
//		}
//		
//		return result;
//	}
//	
//	public synchronized void addShuffle(Word word) {
//		if(!shuffle.containsKey(word.getWord())) {
//			shuffle.put(word.getWord(), new ArrayList<Word>());
//		}
//		
//		shuffle.get(word.getWord()).add(word);
//	}
//	
//	public synchronized void addReduce(Word word) {
//		reduced.add(word);
//	}
//	
//	class MapWorker implements Runnable {
//
//		private List<Word> words;
//		private String line;
//		
//		public MapWorker(String line) {
//			this.line = line;
//		}
//		
//		@Override
//		public void run() {
//			System.out.println("START");
//			words = new ArrayList<>();
//			
//			for(String word : line.split(" ")) {
//				word = word.replace("\r", "");
//				
//				if(!word.equals(""))
//					words.add(new Word(word.toLowerCase(), 1));
//			}
//			
//			for(Word word : words) {
//				addShuffle(word);
//			}
//			
//			System.out.println("END");
//		}
//		
//	}
//	
//	class ReduceWorker implements Runnable {
//
//		private List<Word> words;
//		
//		public ReduceWorker(List<Word> words) {
//			this.words = words;
//		}
//		
//		@Override
//		public void run() {
//			Word word = words.get(0);
//			addReduce(new Word(word.getWord(), words.size()));
//		}
//		
//	}
//
//}
