/**
 * Alex McCanna
 * CSCD 467 Final Project
 * Finding Friends using Hadoop MapReduce
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class FriendsList
{
	public static class TokenMapper extends Mapper<Object, Text, Text, Text>
	{
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 * Map method for MapReduce process. Takes incoming line and parses it into a key and a value.
		 * The key consists of the first person (string) and a second person from their friends list.
		 * The value is everything after the first person, with comma separation added for readability further on.
		 * This is then written to the context and sent to the Reduce function. 
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    {
	    	Text word = new Text();
			String stringWord = new String();
			Text val = new Text();
    		String in = value.toString();
	    	String[] strings = in.split(" ");
			String person1 = strings[0];
			for (int x=1; x<strings.length; x++)
			{
				if(x != (strings.length-1))
						stringWord += strings[x] + ", ";
				else
					stringWord += strings[x];
			}
			for (int x=1; x<strings.length; x++)
			{
				if(strings[x].compareTo(person1) < 0)
				{
					word.set(strings[x]+ "-" + person1);
				}
				else
				{
					word.set(person1 + "-" +strings[x]);
				}
				val.set(stringWord);
				context.write(word, val);
			}
	    }
	}
	
	
	public static class ListReducer extends Reducer<Text, Text, Text, Text>
	{
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 * Takes in the key and values generated in the Map function. There will be two KV pairs per entry.
		 * The values are then split into individual components by spaces, and the subsequent arrays are "intersected".
		 * This produces the common friends, which is then written to context, and the HDFS will take care of the rest.
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			String f = new String();
			String u = new String();
			Iterator<Text> it = values.iterator();
			Text t = it.next();
			f = t.toString();
			if(it.hasNext())
			{
				Text e = it.next();
				u = e.toString();
			}
			
			String[] fSplit = f.split(" ");
			String[] uSplit = u.split(" ");
			
			String c = new String();
			for(String m : fSplit)
			{
				c += (m + " ");
			}
			for(String n : uSplit)
			{
				if(c.contains(n))
				{
					c += n;	
				}
			}
			
			String[] cc = c.split(" ");
			c =  cc[cc.length-1];
			
			
			Text s = new Text(c);
			
			context.write(key, s);
		}
	}
		
		
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: wordcount <in> [<in>...] <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Friends list");
		
		job.setJarByClass(FriendsList.class);
		job.setMapperClass(TokenMapper.class);
		job.setCombinerClass(ListReducer.class);
		job.setReducerClass(ListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

	
	
}