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

	public static class MapperClass extends Mapper<Text, MapWritable, Text, TextMapWritable>{
		private Map<String, Double> featureCounts;
        private Map<String, Double> lemmaCounts;
		private Double F = 0.0;
		private Double L = 0.0;
        private Text newKey = new Text();
		private TextMapWritable newVal;
		private Map<String, Map<String, String>> wordPairsMap = new HashMap<String, Map<String, String>>();

		protected void setup(Context context) throws IOException {
            featureCounts = parseJsonFromSQS(Env.PROJECT_NAME +"-feature");
            lemmaCounts = parseJsonFromSQS(Env.PROJECT_NAME +"-lemmata");
			featureCounts.forEach( (feature, count) -> F += count);
			lemmaCounts.forEach( (word, count) -> L += count);
			processWordPairsFromS3(wordPairsMap);
		}

        private static HashMap<String, Double> parseJsonFromSQS(String SQS_name) {
            Gson gson = new Gson();
            String featureSet = AWS.getInstance().checkSQSQueue(SQS_name);
            Type type = new TypeToken<HashMap<String, Object>>() {}.getType();
            return gson.fromJson(featureSet, type);
        }

		public static void processWordPairsFromS3(Map<String, Map<String, String>> wordPairsMap) throws IOException {
			String localDir = "/tmp";
			String localFilePath = localDir + "/" + Env.wordRelatednessKey;
	
			// Ensure local directory exists
			File directory = new File(localDir);
			if (!directory.exists()) {
				directory.mkdirs();
			}
	
			// Download the file from S3
			AWS.getInstance().downloadFromS3(Env.PROJECT_NAME, Env.wordRelatednessKey, localDir);
	
			// Read the file and populate the map
			try (BufferedReader reader = new BufferedReader(new FileReader(localFilePath))) {
				String line;
	
				while ((line = reader.readLine()) != null) {
					// Process each line
					String[] parts = line.split("\t");
					if (parts.length == 3) {
						String w1 = parts[0].toLowerCase();
						String w2 = parts[1].toLowerCase();
	
						// Add the word pair to the map
						wordPairsMap.putIfAbsent(w1, new HashMap<>());
						wordPairsMap.get(w1).put(w2, "second");
	
						wordPairsMap.putIfAbsent(w2, new HashMap<>());
						wordPairsMap.get(w2).put(w1, "first");
					}
				}
			}
		}
		
		@Override
		public void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
            HashMap<String, Double> map = convertMap(value);
			MapWritable newMap = new MapWritable();
			String word = key.toString();
			Double l = lemmaCounts.get(word);
			map.forEach((feature, count) -> {						//TODO: solve dividing by zero and log of zero 
				Double f = featureCounts.get(feature);
				ArrayList<DoubleWritable> equations = new ArrayList<DoubleWritable>();
				equations.add(new DoubleWritable(count));									 //N5 : assoc_freq(l,f) = Count(F=f,L=l)
				equations.add(new DoubleWritable(count/l));									 //N6 : assoc_prob(l,f) = P(F=f|L=l) = Count(F=f,L=l)/Count(L=l)
				equations.add(new DoubleWritable(Math.log((count/L) / (l/L)*(f/F))));		 //N7 : assoc_PMI(l,f) = log2(P(F=f,L=l)/(P(L=l)*P(F=f)))
				equations.add(new DoubleWritable(((count/L) - (l/L)*(f/F) / Math.sqrt(0)))); //N8 : assoc_t-test(l,f) = (P(F=f,L=l) - P(L=l)*P(F=f))/sqrt(P(L=l)*P(F=f))
				ArrayWritable WriteableArray = new ArrayWritable(DoubleWritable.class);
				WriteableArray.set(equations.toArray(new DoubleWritable[0]));
				newMap.put(new Text(feature), WriteableArray);
			});

			// Iterate over all associated w2 keys and process them
			for (Map.Entry<String, String> entry : wordPairsMap.get(word).entrySet()) {
				String w2 = entry.getKey();
				String relation = entry.getValue(); // "first" or "second" the relation between w1, w2
													// w1,w2 meaning that w2 is second while w2,w1 means w2 is first
				switch (relation) {
					case "first":
						newKey.set(w2 + " " + word);
						newVal = new TextMapWritable(new Text("second") , newMap);
						context.write(newKey, newVal);
						break;
					case "second":
						newKey.set(word + " " + w2);
						newVal = new TextMapWritable(new Text("first") , newMap);
						context.write(newKey, newVal);
						break;
					default:
						break;
				}


			}

        }

        private HashMap<String, Double> convertMap(MapWritable value){
            HashMap<String, Double> valueMap = new HashMap<>();
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                String featureKey = entry.getKey().toString();
                Double count = ((DoubleWritable) entry.getValue()).get();
                valueMap.put(featureKey, count);
            }
            return valueMap;
        }

    }

	public static class ReducerClass extends Reducer<Text, TextMapWritable, Text, ArrayWritable> {
		private Text newKey = new Text();
		private ArrayWritable newValue = new ArrayWritable(DoubleWritable.class);
	
		@Override
		public void reduce(Text key, Iterable<TextMapWritable> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> w1Map;
			HashMap<String, Double> w2Map;
			for (TextMapWritable val : values) {
				String position = val.getText().toString();
				switch (position) {
					case "first":
						w1Map = convertMap(val.getMap());
					break;
					case "second":
						w2Map = convertMap(val.getMap());
					break;
					default:
						break;
				}
				
			}
        }

		private HashMap<String, Double> convertMap(MapWritable value){
            HashMap<String, Double> valueMap = new HashMap<>();
            for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
                String featureKey = entry.getKey().toString();
                Double count = ((DoubleWritable) entry.getValue()).get();
                valueMap.put(featureKey, count);
            }
            return valueMap;
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