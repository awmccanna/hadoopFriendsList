/**
 * Alex McCanna
 * CSCD 467 Final Project
 * Finding Friends using hadoop MapReduce
 */






import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;




public class FriendsList
{
	
	public static class tokenMapper extends Mapper<Object, Text, Text, Text>
	{
	    private Text word = new Text();
	    private Text val = new Text();
	      
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    {
	    	//In case I can't guarantee one line at a time, tokeninze by \n, then run the following code on each token
	    	StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
	    	while(itr.hasMoreTokens())
	    	{
	    		String in = itr.nextToken();
		    	String[] strings = in.split(" ");
		      
				for(String s : strings)
				{	
					String[] result = s.split(" ");
					String s1 = new String();
					String c1 = result[0];
					for (int x=1; x<result.length; x++)
					{
						s1 += result[x] + " ";
					}
					
					for (int x=1; x<result.length; x++)
					{
						if(result[x].compareTo(c1) < 0)
						{
							word.set(result[x]+c1);
						}
						else
						{
							word.set(c1+result[x]);
						}
					}
					val.set(s1);
					context.write(word, val);
				}
	    	}
	    }
	}
	
	public static class listReducer extends Reducer<Text, Text, Text, Text>
	{
		
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
		job.setMapperClass(tokenMapper.class);
		job.setCombinerClass(listReducer.class);
		job.setReducerClass(listReducer.class);
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
