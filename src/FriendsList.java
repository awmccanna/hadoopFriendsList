/**
 * Alex McCanna
 * CSCD 467 Final Project
 * Finding Friends using hadoop MapReduce
 */






import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    {
	    	Text word = new Text();
			String sVal = new String();
			Text val = new Text();
    		String in = value.toString();
	    	String[] strings = in.split(" ");
			String c1 = strings[0];
			for (int x=1; x<strings.length; x++)
			{
				sVal += strings[x] + " ";
			}
			for (int x=1; x<strings.length; x++)
			{
				if(strings[x].compareTo(c1) < 0)
				{
					word.set(strings[x]+c1);
				}
				else
				{
					word.set(c1+strings[x]);
				}
				val.set(sVal);
				context.write(word, val);
			}
			
			
	    }
	}
	
	
	public static class ListReducer extends Reducer<Text, Text, Text, Text>
	{
		
		
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
			
			List<String> k = Arrays.asList(fSplit);
			List<String> r = Arrays.asList(uSplit);
			
			String c = new String();
			for(String m : k)
			{
				c += m + " ";
			}
			
			for(String n : r)
			{
				if(c.contains(n))
				{
					c += n;	
				}
			}
			
			String[] cc = c.split(" ");
			c =  cc[cc.length-1] ;
			
			
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