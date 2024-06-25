package brcChallenge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class BRCStreams {

	public static void main(String[] args) {
		
		long start = System.currentTimeMillis();
		
		String fileName = "/Users/saieeshabonthala/Downloads/measurementsBig.txt";
		
		try {
			ConcurrentHashMap<String, details> map = dataProcessing(fileName);
			String result = outputStream(map);
			System.out.print(result);
			
		}catch (IOException e) {
			e.printStackTrace();
			System.err.println("Error processing the file: " + e.getMessage());
		}
		
		long end = System.currentTimeMillis();
        long executionTime = end - start;

        System.out.println("Execution time: " + executionTime + " milliseconds");

	}
	
	public static ConcurrentHashMap<String, details> dataProcessing(String fileName) throws IOException{
		
		ConcurrentHashMap<String, details> map = new ConcurrentHashMap<>();
		
		try(BufferedReader reader = new BufferedReader(new FileReader(fileName))){
			reader.lines()
				.parallel()
				.forEach(line -> {
				int delimiter = line.indexOf(";");
				String weatherName = line.substring(0, delimiter);
				String valueNum = line.substring(delimiter + 1);
				Double value = Double.parseDouble(valueNum);
				map.computeIfAbsent(weatherName, k -> new details()).addDetails(value);
				});
		}
		
		return map;
		
	}
	
	public static String outputStream(ConcurrentHashMap<String, details> map) {
		return "{" + map.entrySet().stream()
				.sorted(Map.Entry.comparingByKey())
				.map(entry -> entry.getKey() + "=" + entry.getValue())
				.collect(Collectors.joining(", ")) +
				"}";
		
	}
	
	
	private static class details {
		private double min = Double.MAX_VALUE;
		private double max = Double.MIN_VALUE;
		private double sum = 0;
		private int count = 0;
		
		
		public void addDetails(double value){
			min = Math.min(min, value);
			max = Math.max(max, value);
			sum = sum + value;
			count++;
		}
		
		@Override
		public String toString() {
			double mean = sum/count;
			return String.format("%.1f/%.1f/%.1f", min, mean, max);
		}
		
		
	}

}
