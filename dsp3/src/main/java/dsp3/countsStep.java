package dsp3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dsp3.countsStep.MapperClass;


public class countsStep 
{

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		private Map<String, Map<String, Boolean>> wordPairsMap = new HashMap<>();
		private Text newKey = new Text();
		private Text newVal = new Text();

		protected void setup(Context context) throws IOException {
			String filePath = getClass().getResource("/resources/word-relatedness.txt").getPath();

			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line;

			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					String w1 = parts[0];
					String w2 = parts[1];
					boolean value = Boolean.parseBoolean(parts[2]);

					wordPairsMap.putIfAbsent(w1, new HashMap<>());
					wordPairsMap.get(w1).put(w2, value);
				}
			}
			reader.close();
		}
		

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] parts = value.toString().split(Env.TAB);

			String[] wordsInfo = parts[1].toString().split(Env.SPACE);
			
			// Extract only the words and their positions from the dependency parse
			List<WordPosition> words = new ArrayList<>();
			for (int i = 0; i < wordsInfo.length; i++) {
				String[] wordInfo = wordsInfo[i].split(Env.FORWARD_SLASH);
				String word = wordInfo[0].toLowerCase();
				String relation = wordInfo[1].toLowerCase();
				int relatedTo = Integer.parseInt(wordInfo[3]);
				int position = i;
				words.add(new WordPosition(word, position, relation, relatedTo));
			
			}
			
			// Sort words according to compareTo func
			Collections.sort(words);
			
			for (int i = 0; i < words.size(); i++) {
				String word1 = words.get(i).word;
				
				if (wordPairsMap.containsKey(word1)) {
					// Check all other words that come after word1 in the sentence ????????????????
					for (int j = i + 1; j < words.size(); j++) {
						String word2 = words.get(j).word;
						
						if (wordPairsMap.get(word1).containsKey(word2)) {
							addCounts_etc(word1, word2);
							// context.write("bla", "bla");
						}
					}
				}
			}
		}
		
		private static class WordPosition implements Comparable<WordPosition> {
			String word;
			int position;
			String relation;
			int relatedTo;
		
			WordPosition(String word, int position, String relation, int relatedTo) {
				this.word = word;
				this.position = position;
				this.relation = relation;
				this.relatedTo = relatedTo;
			}
		
			@Override
			public int compareTo(WordPosition other) {
				// Compare based on position
				return Integer.compare(this.position, other.position);
			}
		}
		
		// This is a placeholder for the exists function that will be implemented later
		private void addCounts_etc(String word1, String word2) {
			// Implementation will be provided later
			// context.write("bla", "bla");
		}   
	}
	public static class ReducerClass extends Reducer<Text,Text,Text,Text>{
		private Text newKey = new Text();
		private Text newVal = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
           
        }
    }
	
	public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (numPartitions == 0) ? 0 : Math.abs(key.hashCode() % numPartitions);
        }
    }

	public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
		private Text combinedValue = new Text();
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("bucket_name", bucketName);
		Job job = Job.getInstance(conf, args[0]);
		job.setJarByClass(countsStep.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class); //FOR HEB-3GRAMS
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
