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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.tartarus.snowball.ext.PorterStemmer;



public class calcProbabilityStep {

	public static class MapperClass extends Mapper<Text, Text, Text, Text>{
		private ArrayList<String> wordsToCalculate = new ArrayList<String>();
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
			PorterStemmer stemmer = new PorterStemmer();
			while ((line = reader.readLine()) != null) {
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					String w1 = parts[0].toLowerCase();
					String w2 = parts[1].toLowerCase();
					stemmer.setCurrent(w1);
					stemmer.stem();
					wordsToCalculate.add(stemmer.getCurrent());
					stemmer.setCurrent(w2);
					stemmer.stem();
					wordsToCalculate.add(stemmer.getCurrent());
				}
			}
			reader.close();
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String word = key.toString().split(Env.SPACE)[0];
			if(wordsToCalculate.contains(word)){
				context.write(key, value);
			}
        }


    }

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private Double F = 0.0;
		private Double L = 0.0;
		private Text newKey = new Text();
		private Text newValue = new Text();

		protected void setup(Context context) throws IOException {
            F = parseCountFromSQS(Env.F);
			L = parseCountFromSQS(Env.L);
		}

        private static Double parseCountFromSQS(String SQS_name) {
            String count = "";
			while(count == ""){
				count = AWS.getInstance().checkSQSQueue(SQS_name);
			}
            return Double.parseDouble(count);
        }
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Double[] counts = new Double[]{0.0, 0.0, 0.0}; // countf countl countlf
			for (Text val : values) {
				String[] parts = val.toString().split(Env.SPACE);
				if (parts.length < 2) {
					System.err.println("Invalid input format: " + val.toString());
					continue; 
				}
				try {
					double value = Double.parseDouble(parts[1]);
					switch (parts[0]) {
						case "count(l,f)":
							counts[2] = value;
							break;
						case "count(l)":
							counts[1] = value;
							break;
						case "count(f)":
							counts[0] = value;
							break;
						default:
							System.err.println("Unexpected key: " + parts[0]);
					}
				} catch (NumberFormatException e) {
					System.err.println("Invalid number format: " + parts[1]);
				}
			}
			Double[] measures = getMeasures(counts);
			String[] word_feature = key.toString().split(Env.SPACE);

			newKey.set(word_feature[0]);
			newValue.set(
						word_feature[1] + " " + 
						String.valueOf(measures[0]) + " " +
						String.valueOf(measures[1]) + " " +
						String.valueOf(measures[2]) + " " +
						String.valueOf(measures[3])
						);
			context.write(newKey, newValue);
		}

		private Double[] getMeasures(Double[] counts){
			Double[] measures = new Double[4];
			measures[0] = counts[2]; // count(l,f)
			measures[1] = (counts[1] == 0) ? 0.0 : counts[2]/counts[1]; // count(l,f)/count(l)
			Double a = counts[2]/F;  // count(l,f)/count(F)
			Double b = (counts[0] == 0) ? 0.0 : ((counts[1] / L) * (counts[0] / F)); //(count(l)/count(L)) * (count(f)/count(F))

			measures[2] = (b == 0) ? 0.0 : Math.log(a/b); 
			measures[3] = (b == 0) ? 0.0 : (a-b)/Math.sqrt(b);
			return measures;
		}

	}
	
	public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
			String[] parts = key.toString().split(" ");
			String wordPair = parts[0] + parts[1];
            return (numPartitions == 0) ? 0 : Math.abs(wordPair.hashCode() % numPartitions);
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

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}