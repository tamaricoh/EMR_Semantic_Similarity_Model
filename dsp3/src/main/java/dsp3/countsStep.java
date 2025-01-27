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
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.tartarus.snowball.ext.PorterStemmer;



public class countsStep {

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text>{
		private Text newKey = new Text();
		private Text newVal = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] parts = value.toString().split(Env.TAB);

			String count = parts[2];
			
			String[] wordsInfo = parts[1].toString().split(Env.SPACE);
			
			// Extract only the words and their positions from the dependency parse
			List<WordPosition> words = new ArrayList<>();
			PorterStemmer stemmer = new PorterStemmer(); // Create an instance of the stemmer

			for (int i = 0; i < wordsInfo.length; i++) {
				String[] wordInfo = wordsInfo[i].split(Env.FORWARD_SLASH);
				if (wordInfo.length >= 4) {
					String originalWord = wordInfo[0].toLowerCase();
					
					// Apply the stemmer to the original word
					stemmer.setCurrent(originalWord);
					stemmer.stem();
					String stemmedWord = stemmer.getCurrent(); // Get the stemmed word
					
					String dependencyLabel = wordInfo[1].toLowerCase();
					int relatedTo = Integer.parseInt(wordInfo[3]);
					int position = i + 1;
					
					// Add the stemmed word instead of the original word
					words.add(new WordPosition(stemmedWord, position, dependencyLabel, relatedTo));
				} else {
					System.err.println("[Tamar] Warning: Skipping malformed word info: " + wordsInfo[i]);
				}
			}

			
			// Sort words according to compareTo func
			Collections.sort(words);
			List<String> relatedFeatures = new ArrayList<>();

			for (int i = 0; i < words.size(); i++) {
				WordPosition word1 = words.get(i);
				for (int j = 0; j < words.size(); j++) {
					WordPosition word2 = words.get(j);
					
					if (word1.position == word2.relatedTo) {
						// count(F)
						newKey.set("count(F)");
						newVal.set(count);
						context.write(newKey, newVal);

						// count(l,f)
						String f = word2.word + Env.DASH + word2.dependencyLabel;
						newKey.set("count(l,f)" + Env.SPACE + word1.word + Env.SPACE + f);
						newVal.set(count);
						context.write(newKey, newVal);

						// count(f)
						newKey.set("count(f)" + Env.SPACE + f);
						newVal.set(count + Env.SPACE +  word1.word);
						context.write(newKey, newVal);

						relatedFeatures.add(f);

					}

					// count(L)
					newKey.set("count(L)");
					newVal.set(count);
					context.write(newKey, newVal);

					// count(l)
					for (String relatedFeature : relatedFeatures){
						newKey.set("count(l)" + Env.SPACE + word1.word); 
						double countDouble = Double.parseDouble(count);
						double result = countDouble / relatedFeatures.size();
						String resultString = Double.toString(result);
						newVal.set(resultString + Env.SPACE + relatedFeature);
						context.write(newKey, newVal);
					}
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

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private Text newKey = new Text();
		private Text newVal = new Text();
		static AWS aws = AWS.getInstance();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] parts = key.toString().split(Env.SPACE);
			String keyKind = parts[0];
			ArrayList<String> vals = toArray(values);
			

			switch (keyKind){
				case "count(F)":
					aws.sendSQSMessage(Env.F, sum(vals));
					break;
				case "count(L)":
					aws.sendSQSMessage(Env.L, sum(vals));
					break;
				case "count(l,f)":
					newKey.set(parts[1] + Env.SPACE + parts[2]);
					newVal.set(keyKind + Env.SPACE + sum(vals));
					context.write(newKey, newVal);
					break;
				case "count(l)":
					newVal.set(keyKind + Env.SPACE + sum(vals));
					for (String val : vals){
						String f = val.split(Env.SPACE)[1];
						newKey.set(parts[1] + Env.SPACE + f);
						context.write(newKey, newVal);
					}
					break;
				case "count(f)":
					newVal.set(keyKind + Env.SPACE + sum(vals));
					for (String val : vals){
						String l = val.split(Env.SPACE)[1];
						newKey.set(l + Env.SPACE + parts[2]);
						context.write(newKey, newVal);
					}
					break;
			}
		}

		private ArrayList<String> toArray(Iterable<Text> values){
			ArrayList<String> output = new ArrayList<String>();
			for (Text val : values) {
				output.add(val.toString());
			}
			return output;
		}

		private String sum(ArrayList<String> values){
			Double sum = 0.0;
			for (String val : values) {
				sum += Double.parseDouble(val.split(Env.SPACE)[0]);
			}

			return String.valueOf(sum);	
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
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
