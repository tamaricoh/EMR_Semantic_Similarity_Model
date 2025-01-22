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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;



public class calcProbabilityStep {

	public static class MapperClass extends Mapper<Text, MapWritable, Text, Text>{
		private Map<String, Double> featureCounts;
        private Map<String, Double> lemmaCounts;
        private Text newKey = new Text();
        private Text newValue = new Text();

		protected void setup(Context context) throws IOException {
            featureCounts = parseJsonFromSQS(Env.PROJECT_NAME +"-feature");
            lemmaCounts = parseJsonFromSQS(Env.PROJECT_NAME +"-lemmata");
		}

        private static HashMap<String, Double> parseJsonFromSQS(String SQS_name) {
            Gson gson = new Gson();
            String featureSet = AWS.getInstance().checkSQSQueue(SQS_name);
            Type type = new TypeToken<HashMap<String, Object>>() {}.getType();
            return gson.fromJson(featureSet, type);
        }
		

		@Override
		public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();
            
            // Convert MapWritable to a regular HashMap for easier handling
            Map<String, Integer> valueMap = convertMap(value);

            // Handle special cases from previous step
            if (keyStr.equals("Feature")) {
                // Broadcast feature counts to all reducers with special prefix
                newKey.set("##FEATURES##");
                newValue.set(new Gson().toJson(valueMap));
                context.write(newKey, newValue);
            } else if (keyStr.equals("Lemmata")) {
                // Broadcast lemma counts to all reducers with special prefix
                newKey.set("##LEMMAS##");
                newValue.set(new Gson().toJson(valueMap));
                context.write(newKey, newValue);
            } else {
                // Regular word entry - emit for similarity calculation
                newKey.set(keyStr);
                newValue.set(new Gson().toJson(valueMap));
                context.write(newKey, newValue);
            }
        }

        private Map<String, Integer> convertMap(MapWritable value){
            Map<String, Integer> valueMap = new HashMap<>();
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                String featureKey = entry.getKey().toString();
                Integer count = ((IntWritable) entry.getValue()).get();
                valueMap.put(featureKey, count);
            }
            return valueMap;
        }
    
    }

	public static class ReducerClass extends Reducer<Text, Text, Text, MapWritable> {
		private Text feature = new Text();
		private IntWritable zero = new IntWritable(0);
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}