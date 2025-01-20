package dsp3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dsp3.countsStep.MapperClass;
import dsp3.countsStep.ReducerClass;


public class countsStep 
{

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		private Map<String, Map<String, Integer>> wordPairsMap = new HashMap<>();
		private Text newKey = new Text();
		private Text newVal = new Text();

		protected void setup(Context context) throws IOException {
			String filePath = getClass().getResource("/resources/word-relatedness.txt").getPath();

			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line;

			while ((line = reader.readLine()) != null) {
				// Stemming
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					String w1 = parts[0].toLowerCase();
					String w2 = parts[1].toLowerCase();
					// boolean value = Boolean.parseBoolean(parts[2]);

					wordPairsMap.putIfAbsent(w1, new HashMap<>());
					wordPairsMap.get(w1).put(w2, 0);
					wordPairsMap.putIfAbsent(w2, new HashMap<>());
					wordPairsMap.get(w2).put(w1, 0);
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
				// utilize stemmer ======================================
				String[] wordInfo = wordsInfo[i].split(Env.FORWARD_SLASH);
				String word = wordInfo[0].toLowerCase();
				String dependencyLabel = wordInfo[1].toLowerCase();
				int relatedTo = Integer.parseInt(wordInfo[3]);
				int position = i+1;
				words.add(new WordPosition(word, position, dependencyLabel, relatedTo));
			}
			
			// Sort words according to compareTo func
			Collections.sort(words);
			
			for (int i = 0; i < words.size(); i++) {
				WordPosition word1 = words.get(i);
				
				if (wordPairsMap.containsKey(word1.word)) {
					for (int j = 0; j < words.size(); j++) {
						WordPosition word2 = words.get(j);
						
						if (word1.position == word2.relatedTo && wordPairsMap.get(word1.word).containsKey(word2.word)) {

							// Random Variable F
							newKey.set("Feature"); // Uppercase letter to diffrentiate from word in corpus.
							newVal.set(word2.word + Env.DASH + word2.dependencyLabel + Env.DASH + parts[2]);
							context.write(newKey, newVal);

							// Random Variable F,L
							newKey.set(word1.word);
							newVal.set(word2.word + Env.DASH + word2.dependencyLabel + Env.DASH + parts[2]);
							context.write(newKey, newVal);
						}
					}
					// Random Variable L
					newKey.set("Lemmata"); // Uppercase letter to diffrentiate from word in corpus.
					newVal.set(word1.word + Env.DASH + Env.DASH + parts[2]);
					context.write(newKey, newVal);
				}
			}
		}
		
		private static class WordPosition implements Comparable<WordPosition> {
			String word;
			int position;
			String dependencyLabel;
			int relatedTo;
		
			WordPosition(String word, int position, String dependencyLabel, int relatedTo) {
				this.word = word;
				this.position = position;
				this.dependencyLabel = dependencyLabel;
				this.relatedTo = relatedTo;
			}
		
			@Override
			public int compareTo(WordPosition other) {
				// Compare based on position
				return Integer.compare(this.position, other.position);
			}
		}  
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, MapWritable> {
		private Text feature = new Text();
		private IntWritable zero = new IntWritable(0);
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			MapWritable featureSet = new MapWritable();
	
			for (Text val : values) {
				// Parse the input value
				String[] parts = val.toString().split(Env.DASH);
				feature.set(parts[0] + Env.DASH + parts[1]);
				IntWritable currentCount = new IntWritable(Integer.parseInt(parts[2]));
	
				// Calculate the updated count
				IntWritable existingCount = (IntWritable) featureSet.getOrDefault(feature, zero);
				IntWritable updatedCount = new IntWritable(existingCount.get() + currentCount.get());
	
				// Update the featureSet with the new count
				featureSet.put(new Text(feature), updatedCount);
			}
	
			// Write the key and the updated featureSet to the context
			context.write(key, featureSet);
		}
	}
	
	public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (numPartitions == 0) ? 0 : Math.abs(key.hashCode() % numPartitions);
        }
    }

	public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
		private Text feature = new Text();
		private IntWritable zero = new IntWritable(0);
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			MapWritable featureSet = new MapWritable();
	
			for (Text val : values) {
				// Parse the input value
				String[] parts = val.toString().split(Env.DASH);
				feature.set(parts[0] + Env.DASH + parts[1]);
				IntWritable currentCount = new IntWritable(Integer.parseInt(parts[2]));
	
				// Calculate the updated count
				IntWritable existingCount = (IntWritable) featureSet.getOrDefault(feature, zero);
				IntWritable updatedCount = new IntWritable(existingCount.get() + currentCount.get());
	
				// Update the featureSet with the new count
				featureSet.put(new Text(feature), updatedCount);
			}
			Text val = new Text();
			for (Writable featureKey : featureSet.keySet()) {
				val.set(((Text) featureKey).toString() + Env.DASH + String.valueOf(featureSet.get(featureKey)));
				context.write(key, val);
			}
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
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
