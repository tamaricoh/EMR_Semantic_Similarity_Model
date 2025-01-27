package dsp3;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
import org.codehaus.jackson.util.TextBuffer;
import org.apache.hadoop.io.ArrayWritable;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;



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

			while ((line = reader.readLine()) != null) {
				// Stemming
				String[] parts = line.split("\t");
				if (parts.length == 3) {
					String w1 = parts[0].toLowerCase();
					String w2 = parts[1].toLowerCase();
					wordsToCalculate.add(w1);
					wordsToCalculate.add(w2);
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
            String count = AWS.getInstance().checkSQSQueue(SQS_name);
            return Double.parseDouble(count);
        }
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> counts = toMap(values);
			Double count_f =
			Double count_l
			Double count_l_f
				
		}

		private ArrayList<String> toArray(Iterable<Text> values){
			HashMap<String, Double> output = new HashMap<>();
			for (Text val : values) {
				String[] parts = val.toString().split(Env.SPACE);
				output.put(parts[0], Double.parseDouble(parts[1]));
			}
			return output;
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

	public static class CombinerClass extends Reducer<Text, Text, Text, Text> {
		private Text feature = new Text();
		private IntWritable zero = new IntWritable(0);
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		}
	}

	public static class TextMapWritable implements Writable {
		private Text text;
		private MapWritable map;
	
		// Default constructor (required by Hadoop)
		public TextMapWritable() {
			this.text = new Text();
			this.map = new MapWritable();
		}
	
		// Constructor to initialize fields
		public TextMapWritable(Text text, MapWritable map) {
			this.text = text;
			this.map = map;
		}
	
		// Getters and setters
		public Text getText() {
			return text;
		}
	
		public void setText(Text text) {
			this.text = text;
		}
	
		public MapWritable getMap() {
			return map;
		}
	
		public void setMap(MapWritable map) {
			this.map = map;
		}
	
		// Serialize the object
		@Override
		public void write(DataOutput out) throws IOException {
			text.write(out);
			map.write(out);
		}
	
		// Deserialize the object
		@Override
		public void readFields(DataInput in) throws IOException {
			text.readFields(in);
			map.readFields(in);
		}
	
		@Override
		public String toString() {
			return "TextMapWritable{" +
					"text=" + text.toString() +
					", map=" + map.toString() +
					'}';
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