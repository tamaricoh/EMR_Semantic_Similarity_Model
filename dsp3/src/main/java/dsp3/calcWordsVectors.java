package dsp3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.s;
import org.tartarus.snowball.ext.PorterStemmer;

import dsp3.calcWordsVectors.wordPairs;
import java_cup.parse_action_row;

public class calcWordsVectors {

    public static class wordPairs {
        String w1;
        String w2;
    
        // Constructor
        public wordPairs(String w1, String w2) {
            this.w1 = w1;
            this.w2 = w2;
        }
    
        // Method to get the first word
        public String getFirst() {
            return this.w1;
        }
    
        // Method to get the second word
        public String getSecond() {
            return this.w2;
        }
    
        // Method to check if a word is either w1 or w2
        public boolean contains(String w) {
            return w.equals(this.w1) || w.equals(this.w2);
        }

        // Static method to filter wordPairs containing the given word
        public static ArrayList<wordPairs> filterByWord(String word, ArrayList<wordPairs> pairs) {
            ArrayList<wordPairs> result = new ArrayList<>();
            for (wordPairs pair : pairs) {
                if (pair.contains(word)) {
                    result.add(pair);
                }
            }
            return result;
        }  
    }

	public static class MapperClass extends Mapper<Text, Text, Text, Text>{
		private ArrayList<wordPairs> wordspairs = new ArrayList<wordPairs>();
		static AWS aws = AWS.getInstance();
        private Text newKey = new Text();
        private Text newVal = new Text();

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
					w1 = stemmer.getCurrent();
					stemmer.setCurrent(w2);
					stemmer.stem();
					w2 = stemmer.getCurrent();
                    wordPairs pair = new wordPairs(w1, w2);
                    wordspairs.add(pair);
				}
			}
			reader.close();
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String word = key.toString();           //oldkey: w       oldVal: feature p1 p2 p3 p4
			ArrayList<wordPairs> relatedPairs = wordPairs.filterByWord(word, wordspairs);
            newVal.set(word + Env.space + value.toString());
            for (wordPairs pair : relatedPairs) {
                newKey.set(pair.getFirst() + Env.space + pair.getSecond());
                context.write(newKey, newVal);      // key: w1 w2      val: w feature p1 p2 p3 p4
            }
        }


    }

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private Text newKey = new Text();
		private Text newValue = new Text();
	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] words = key.toString().split(Env.SPACE);
            String first = words[0];
            String second = words[0];
            HashMap<String, Double[]> l1Feature = new HashMap<>();
            HashMap<String, Double[]> l2Feature = new HashMap<>();
            HashMap<String, String> features = new HashMap<>();
            for (Text val : values) {
                String[] value = val.toString().split(Env.SPACE);
                Double[] measures = {
                                     Double.parseDouble(value[2]), 
                                     Double.parseDouble(value[3]), 
                                     Double.parseDouble(value[4]), 
                                     Double.parseDouble(value[5])};
                if(first.equals(value[0])){
                    l1Feature.putIfAbsent(value[1], measures);
                    features.putIfAbsent(value[1], "");
                } else if (second.equals(value[0])) {
                    l2Feature.putIfAbsent(value[1], measures);
                    features.putIfAbsent(value[1], "");
                }
            }
            // Perform calculations for equations 9, 10, 11, 13, 15, and 17
            Double[] calcResult = new Double[24];
            int i = 0;
            for(int position = 0 ; position < 4; position++){
                calcResult[i+0] = calculateManhattanDistance(l1Feature, l2Feature, features, position);
                calcResult[i+1] = calculateEuclideanDistance(l1Feature, l2Feature, features, position);
                calcResult[i+2] = calculateCosineSimilarity(l1Feature, l2Feature, features, position);
                calcResult[i+3] = calculateJaccardSimilarity(l1Feature, l2Feature, features, position);
                calcResult[i+4] = calculateDiceSimilarity(l1Feature, l2Feature, features, position);
                calcResult[i+5] = calculateJensenShannonDivergence(l1Feature, l2Feature, features, position);
                i += 6;
            }

            newValue.set(arrayToString(calcResult));
            context.write(key, newValue);


		}

        private String arrayToString (Double[] arr){
            String result = "";
            for(int i = 0; i<arr.length ; i++){
                result.concat(String.valueOf(arr[i]));
                result.concat(Env.space);
            }
            return result;
        }

        // Example methods for calculations
        //Equation 9
        private double calculateManhattanDistance(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features, int position) {
            double sum = 0.0;
            for (String feature : features.keySet()) {
                if (l1Feature.containsKey(feature) & l2Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    Double[] v2 = l2Feature.get(feature);
                    sum += Math.abs(v1[position] - v2[position]);
                }else if(l1Feature.containsKey(feature)){
                    Double[] v1 = l1Feature.get(feature);
                    sum += Math.abs(v1[position]);
                }
                else if(l2Feature.containsKey(feature)){
                    Double[] v2 = l2Feature.get(feature);
                    sum += Math.abs(v2[position]);
                }
            }
            return sum;
        }

        //Equation 10
        private double calculateEuclideanDistance(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features, int position) {
            double sum = 0.0;
            for (String feature : features.keySet()) {
                if (l1Feature.containsKey(feature) & l2Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    Double[] v2 = l2Feature.get(feature);
                    sum += Math.pow(v1[position] - v2[position], 2);
                }else if(l1Feature.containsKey(feature)){
                    Double[] v1 = l1Feature.get(feature);
                    sum += Math.pow(v1[position], 2);
                }
                else if(l2Feature.containsKey(feature)){
                    Double[] v2 = l2Feature.get(feature);
                    sum += Math.pow(v2[position], 2);
                }
            }
            return Math.sqrt(sum);
        }

        //Equation 11
        private double calculateCosineSimilarity(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features,  int position) {
            double dotProduct = 0.0;
            double normL1 = 1e-100;
            double normL2 = 1e-100;
            for (String feature : features.keySet()) {
                if (l1Feature.containsKey(feature) & l2Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    Double[] v2 = l2Feature.get(feature);
                    dotProduct += v1[position] * v2[position];
                    normL1 += Math.pow(v1[position], 2);
                    normL2 += Math.pow(v2[position], 2);
                } else if (l1Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    normL1 += Math.pow(v1[position], 2);
                } else if (l2Feature.containsKey(feature)) {
                    Double[] v2 = l2Feature.get(feature);
                    normL2 += Math.pow(v2[position], 2);
                } 
            }
            return dotProduct / (Math.sqrt(normL1) * Math.sqrt(normL2));
        }

        //Equation 13
        private double calculateJaccardSimilarity(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features, int position) {
            double min_sum = 0.0;
            double max_sum = 1e-100;
            for (String feature : features.keySet()) {
                if (l1Feature.containsKey(feature) & l2Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    Double[] v2 = l2Feature.get(feature);
                    min_sum += Math.min(v1[position], v2[position]);
                    max_sum += Math.max(v1[position], v2[position]);
                } else if (l1Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    min_sum += Math.min(v1[position], 0.0);
                    max_sum += Math.max(v1[position], 0.0);
                } else if (l2Feature.containsKey(feature)) {
                    Double[] v2 = l2Feature.get(feature);
                    min_sum += Math.min(0.0, v2[position]);
                    max_sum += Math.max(0.0, v2[position]);
                } 
            }
            return min_sum / max_sum;
        }

        //Equation 15
        private double calculateDiceSimilarity(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features, int position) {
            double numenator = 0.0;
            double denumerator = 1e-100;
            for (String feature : features.keySet()) {
                if (l1Feature.containsKey(feature) & l2Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    Double[] v2 = l2Feature.get(feature);
                    numenator += Math.min(v1[position], v2[position]);
                    denumerator += (v1[position] + v2[position]);
                } else if (l1Feature.containsKey(feature)) {
                    Double[] v1 = l1Feature.get(feature);
                    denumerator += v1[position];
                } else if (l2Feature.containsKey(feature)) {
                    Double[] v2 = l2Feature.get(feature);
                    denumerator += v2[position];
                } 
            }
            return 2 * numenator / denumerator;
        }

        //Equation 17 - unclear
        private double calculateJensenShannonDivergence(HashMap<String, Double[]> l1Feature, HashMap<String, Double[]> l2Feature, HashMap<String, String> features, int position) {
            // Calculate midpoint distribution M = (P+Q)/2
            HashMap<String, Double> midpoint = new HashMap<>();
            
            // First calculate midpoint for all features
            for (String feature : features.keySet()) {
                double p1 = 0.0;
                double p2 = 0.0;
                
                if (l1Feature.containsKey(feature)) {
                    p1 = l1Feature.get(feature)[position];
                }
                if (l2Feature.containsKey(feature)) {
                    p2 = l2Feature.get(feature)[position];
                }
                
                midpoint.put(feature, (p1 + p2) / 2.0);
            }
            
            // Calculate divargence = KL(P||M) + KL(Q||M)
            double divergence = 0.0;
            for (String feature : l1Feature.keySet()) {
                if (l1Feature.containsKey(feature)) {
                    double p = l1Feature.get(feature)[position];
                    double m = midpoint.get(feature);
                    divergence += p * Math.log(p / m);
                }
                if (l2Feature.containsKey(feature)) {
                    double q = l2Feature.get(feature)[position];
                    double m = midpoint.get(feature);
                    divergence += q * Math.log(q / m);
                }
            }
            
            return divergence;
        }

	}
	
	public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
			String[] parts = key.toString().split(Env.SPACE);
			String wordPair = parts[0] + parts[1];
            return (numPartitions == 0) ? 0 : Math.abs(wordPair.hashCode() % numPartitions);
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("bucket_name", bucketName);
		Job job = Job.getInstance(conf, args[0]);
		job.setJarByClass(calcWordsVectors.class);

		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
