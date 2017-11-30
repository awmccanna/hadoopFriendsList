/**
 * Alex McCanna
 * CSCD 467 Final Project
 * Finding Friends using hadoop MapReduce
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
	
	public static class TokenMapper extends Mapper<Object, Text, Text, Set<Text>>
	{
	    private Text word = new Text();   
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    {
	    	
    		String in = value.toString();
	    	String[] strings = in.split(" ");
	      
				
			
			Set<Text> holder = new HashSet<Text>();
			String c1 = strings[0];
			for (int x=1; x<strings.length; x++)
			{
				holder.add(new Text(strings[x]));
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
				context.write(word, holder);
			}
			
			
	    }
	}
	
	
	public static class ListReducer extends Reducer<Text, Set<Text>, Text, Set<Text>>
	{
		Set<Text> result = new HashSet<Text>();
		Set<Text> temp = new HashSet<Text>();
		public void reduce(Text key, Iterable<Set<Text>> values, Context context) throws IOException, InterruptedException
		{
			Iterator<Set<Text>> it = values.iterator();
			if(it.hasNext())
			{
				result = it.next();
			}
			
			while(it.hasNext())
			{
				temp = it.next();
				result.retainAll(temp);
			}
			context.write(key, result);
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
		job.setOutputValueClass(Set.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

	
	
}