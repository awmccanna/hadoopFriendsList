import java.io.*;
import java.util.*;

public class DataProcessor
{
	public static void main(String[] args) throws IOException
	{
		File fin = new File("astroPublish");
		FileWriter w = new FileWriter("newAstroPublish");
		FileReader f = new FileReader(fin);
		BufferedReader br = new BufferedReader(f);
		ArrayList<String> strings = new ArrayList<String>();
		Map<String, List<String>> myMap = new HashMap<String, List<String>>();
		
		while(br.ready())
		{
			String line = br.readLine();
			String[] splitLine = line.split(" ");
			String key = splitLine[0];
			String val = splitLine[1];
			put(key, val, myMap);
			put(val, key, myMap);
			
		}
		
		
		
		for(String s : myMap.keySet())
		{
			String toWrite = s;
			List<String> temp = myMap.get(s);
			for(String t : temp)
			{
				toWrite += (" " + t);
			}
			toWrite += "\n";
			w.write(toWrite);
		}
		
		
		br.close();
		w.close();
		
	}

	private static void put(String key, String val, Map<String, List<String>> myMap)
	{
		if(myMap.containsKey(key))
		{
			List<String> temp = myMap.get(key);
			temp.add(val);
		}
		else
		{
			List<String> temp = new ArrayList<String>();
			temp.add(val);
			myMap.put(key, temp);
		}
	}
}
