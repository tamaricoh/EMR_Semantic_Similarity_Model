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
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



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
			for (int i = 0; i < wordsInfo.length; i++) {
				// utilize stemmer ======================================
				String[] wordInfo = wordsInfo[i].split(Env.FORWARD_SLASH);
				if (wordInfo.length >= 4){
					String word = wordInfo[0].toLowerCase();
					String dependencyLabel = wordInfo[1].toLowerCase();
					int relatedTo = Integer.parseInt(wordInfo[3]);
					int position = i+1;
					words.add(new WordPosition(word, position, dependencyLabel, relatedTo));
				}
				else {
					System.err.println("[Tamar] Warning: Skipping malformed word info: " + wordsInfo[i]);
				}
			}
			
			// Sort words according to compareTo func
			Collections.sort(words);
			
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

					}
				// count(L)
				newKey.set("count(L)");
				newVal.set(count);
				context.write(newKey, newVal);

				// count(l)
				newKey.set("count(l)" + Env.SPACE + word1.word); 
				newVal.set(count);
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

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private ArrayList<String> wordToCalculate = new ArrayList<String>();
		private Text newKey = new Text();
		private Text newVal = new Text();
		static AWS aws = AWS.getInstance();

		protected void setup(Context context) throws IOException {
			String localDir = "/tmp";
			String localFilePath = localDir + "/" + Env.wordRelatednessKey;

			File directory = new File(localDir);
			if (!directory.exists()) {
				directory.mkdirs();
			}

			aws.downloadFromS3(Env.PROJECT_NAME, Env.wordRelatednessKey , localDir);

			BufferedReader reader = new BufferedReader(new FileReader(localFilePath));
			String line;

			while ((line = reader.readLine()) != null) {
				// Stemming
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					String w1 = parts[0].toLowerCase();
					String w2 = parts[1].toLowerCase();
					wordToCalculate.add(w1);
					wordToCalculate.add(w2);
				}
			}
			reader.close();
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyKind = key.toString().split(Env.SPACE)[0];
			ArrayList<String> vals = toArray(values);

			switch (keyKind){
				case "count(F)":
					aws.sendSQSMessage(Env.F, sum(vals));
					break;
				case "count(L)":
					aws.sendSQSMessage(Env.L, sum(vals));
					break;
				case "count(l,f)":
					newVal.set(sum(vals));
					context.write(key, newVal);
					break;
				case "count(l)":
					newVal.set(sum(vals));
					context.write(key, newVal);
					break;
				case "count(f)":
					ArrayList<String> relatedWords = filterFromInput(vals);
					newVal.set(sum(vals));
					String f = key.toString().split(Env.SPACE)[1];
					for (String word : relatedWords) {
						newKey.set("count(f)" + Env.SPACE + word + Env.SPACE + f);
						context.write(newKey, key);
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

		private ArrayList<String> filterFromInput(ArrayList<String> values){
			ArrayList<String> filter = new ArrayList<String>();
			for (String val : values) {
				String word = val.split(Env.SPACE)[1];
				if(wordToCalculate.contains(word)){
					filter.add(word);
				}
			}
			return filter;
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
