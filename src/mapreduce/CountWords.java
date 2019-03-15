package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class CountWords {
	
	private List<String> files;
	private Map<String, Integer> mapsResults;
	
	public CountWords(List<String> files) {
		this.files = files;
		this.mapsResults = new TreeMap<>();
	}

	public String execute() {
		String result = "";
		
		for(String file : files) {
			result += file+":\n";
			String line = "";
			//Split
			try (BufferedReader br = new BufferedReader(new FileReader(file))) 
			{
				while((line = br.readLine()) != null) {
					for(String word : line.split(" ")) {
						if(!word.equals("")) {
							word = word.replace("\r", "").toLowerCase();
							if(!mapsResults.containsKey(word))
								mapsResults.put(word, 0);
							
							mapsResults.put(word, mapsResults.get(word)+1);
						}
					}
				}
				
				for (String word : mapsResults.keySet()) {
					System.out.println(word+" : "+mapsResults.get(word));
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		List<String> files = new ArrayList<>();
		for(int i = 0; i < args.length; i++) {
			files.add(args[i]);
		}
		
		long actual = System.currentTimeMillis();
		CountWords count = new CountWords(files);
		System.out.println(count.execute());
		long fin = System.currentTimeMillis();
		System.out.println("Time: "+(fin-actual));
	}
}
