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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.tartarus.snowball.ext.PorterStemmer;

public class countsStep {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newVal = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value == null || value.toString().trim().isEmpty()) {
                return;
            }

            String[] parts = value.toString().split(Env.TAB);
            if (parts.length < 3) {
                System.err.println("[Tamar] Warning: Skipping malformed input line: " + value);
                return;
            }

            String count = parts[2];
			double countDouble = 0.0;
			try {
				countDouble = Double.parseDouble(count);
			} catch (Exception e) {
				System.err.println("[Tamar] Error parsing count: " + count);
			}

            String[] wordsInfo = parts[1].split(Env.SPACE);

            List<WordPosition> words = new ArrayList<>();
            PorterStemmer stemmer = new PorterStemmer();

            for (String wordData : wordsInfo) {
                String[] wordInfo = wordData.split(Env.FORWARD_SLASH);
                if (wordInfo.length < 4) {
                    System.err.println("[Tamar] Warning: Skipping malformed word info: " + wordData);
                    continue;
                }

                String originalWord = wordInfo[0].toLowerCase();
                stemmer.setCurrent(originalWord);
                stemmer.stem();
                String stemmedWord = stemmer.getCurrent();

                String dependencyLabel = wordInfo[1].toLowerCase();
                String relatedToStr = wordInfo[3];

                try {
                    int relatedTo = Integer.parseInt(relatedToStr);
                    words.add(new WordPosition(stemmedWord, words.size() + 1, dependencyLabel, relatedTo));
                } catch (NumberFormatException e) {
                    System.err.println("[Tamar] Skipping invalid relatedTo value: " + relatedToStr);
                }
            }

            Collections.sort(words);
            List<String> relatedFeatures = new ArrayList<>();
			Double countL = 0.0;
			Double countF = 0.0;
            for (WordPosition word1 : words) {
                relatedFeatures.clear();
                for (WordPosition word2 : words) {
                    if (word1.position == word2.relatedTo) {
                        countF = countF + countDouble;

                        String f = word2.word + Env.DASH + word2.dependencyLabel;
                        newKey.set("count(l,f)" + Env.space + word1.word + Env.space + f);
                        newVal.set(count);
                        context.write(newKey, newVal);

                        newKey.set("count(f)" + Env.space + f);
                        newVal.set(count + Env.space + word1.word);
                        context.write(newKey, newVal);

                        relatedFeatures.add(f);
                    }
                }
				countL = countL + countDouble;

                for (String relatedFeature : relatedFeatures) {
                    newKey.set("count(l)" + Env.space + word1.word);
                    double result = countDouble / relatedFeatures.size();
                    newVal.set(String.valueOf(result) + Env.space + relatedFeature);
                    context.write(newKey, newVal);
                }
            }
			newKey.set("count(F)");
            newVal.set(String.valueOf(countF));
            context.write(newKey, newVal);
			newKey.set("count(L)");
            newVal.set(String.valueOf(countL));
            context.write(newKey, newVal);

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
                return Integer.compare(this.position, other.position);
            }
        }
    }

	public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
		private Text newVal = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyStr = key.toString();
			Double sum = 0.0;
			if (keyStr.startsWith("count(F)") || keyStr.startsWith("count(L)") || keyStr.startsWith("count(l,f)") ){
				for (Text val : values) {
					sum = sum + Double.parseDouble(val.toString());
				}
				newVal.set(String.valueOf(sum));
				context.write(key, newVal);
			}
			else {
				for (Text val : values){
					context.write(key, val);
				}
			}
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return (numPartitions == 0) ? 0 : Math.abs(key.hashCode() % numPartitions);
        }
    }
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newVal = new Text();
        static AWS aws = AWS.getInstance();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split(Env.SPACE);
            if (parts.length < 1) {
                return;
            }

            String keyKind = parts[0];
            ArrayList<String> vals = new ArrayList<>();
			Double sum = 0.0;
			String value;
            for (Text val : values) {
				value = val.toString();
                if (val != null) {
					String[] valParts = value.split(Env.SPACE);
                    vals.add(value);
					try {
                        sum += Double.parseDouble(valParts[0]);
                    } catch (NumberFormatException e) {
                        System.err.println("[Tamar] Error parsing value: " + valParts[0]);
                    }
                }
            }
			value = String.valueOf(sum);
			newVal.set(keyKind + Env.space + value);
            if (vals.isEmpty()) {
                return;
            }

            switch (keyKind) {
                case "count(F)":
                    aws.sendSQSMessage(Env.F, value);
                    break;
                case "count(L)":
                    aws.sendSQSMessage(Env.L, value);
                    break;
                case "count(l,f)":
                    if (parts.length >= 3) {
                        // newKey.set(parts[1] + Env.space + parts[2]);
                        // context.write(newKey, newVal);
                    }
                    break;
                case "count(l)":
                    for (String val : vals) {
                        String[] valParts = val.split(Env.SPACE);
                        if (parts.length >= 2 && valParts.length >= 2) {
                            newKey.set(parts[1] + Env.space + valParts[1]);
                            context.write(newKey, newVal);
                        }
                    }
                    break;
                case "count(f)":
                    for (String val : vals) {
                        String[] valParts = val.split(Env.SPACE);
                        if (parts.length >= 2 && valParts.length >= 2) {
                            newKey.set(valParts[1] + Env.space + parts[1]);
                            context.write(newKey, newVal);
                        }
                    }
                    break;
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
		job.setCombinerClass(CombinerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		// FileInputFormat.addInputPath(job, new Path(args[1]));
		for (int i = 0; i < 1; i++) { //TODO
			FileInputFormat.addInputPath(job, new Path(args[1] + i + ".txt"));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
