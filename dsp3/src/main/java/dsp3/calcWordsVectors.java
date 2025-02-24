package dsp3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.tartarus.snowball.ext.PorterStemmer;


public class calcWordsVectors {

    public static class wordPairs {
        String w1;
        String w2;
        String label;
    
        // Constructor
        public wordPairs(String w1, String w2, String label) {
            this.w1 = w1;
            this.w2 = w2;
            this.label = label;
        }
    
        // Getters
        public String getFirst() {
            return this.w1;
        }
    
        public String getSecond() {
            return this.w2;
        }

        public String getLabel() {
            return this.label;
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

        public String toString(){
            return this.getFirst() + Env.space + this.getSecond() + Env.space + this.getLabel();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            wordPairs other = (wordPairs) obj;
            return w1.equals(other.w1) && w2.equals(other.w2) && label.equals(other.label);
        }

        @Override
        public int hashCode() {
            return Objects.hash(w1, w2, label);
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
                    String label = parts[2].toLowerCase();
					stemmer.setCurrent(w1);
					stemmer.stem();
					w1 = stemmer.getCurrent();
					stemmer.setCurrent(w2);
					stemmer.stem();
					w2 = stemmer.getCurrent();
                    wordPairs pair = new wordPairs(w1, w2, label);
                    wordspairs.add(pair);
				}
			}
			reader.close();
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String word = key.toString();           //oldkey: w       oldVal: feature p1 p2 p3 p4
            String[] oldValue = value.toString().split(Env.SPACE);
            String newValue = String.join(Env.space, Arrays.copyOfRange(oldValue, 1, oldValue.length));;
			ArrayList<wordPairs> relatedPairs = wordPairs.filterByWord(word, wordspairs);
            newVal.set(word + Env.space + newValue);
            for (wordPairs pair : relatedPairs) {
                newKey.set(pair.toString() + Env.space + oldValue[0]);
                context.write(newKey, newVal);
                // key: w1 w2 label feature     val: w p1 p2 p3 p4
            }
        }


    }

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private HashMap<wordPairs, double[]> wordspairs = new HashMap<>();
        static AWS aws = AWS.getInstance();
		private Text newValue = new Text();
		private Text newKey = new Text();
        private FSDataOutputStream csvOut;

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
                    String label = parts[2].toLowerCase();
					stemmer.setCurrent(w1);
					stemmer.stem();
					w1 = stemmer.getCurrent();
					stemmer.setCurrent(w2);
					stemmer.stem();
					w2 = stemmer.getCurrent();
                    wordPairs pair = new wordPairs(w1, w2, label);
                    double[] defaultArray = new double[40];
                    Arrays.fill(defaultArray, 0.0);
                    for (int i = 0; i < 4; i++) {
                        defaultArray[(10 * i) + 3] = 1e-100;
                        defaultArray[(10 * i) + 4] = 1e-100;
                        defaultArray[(10 * i) + 6] = 1e-100;
                        defaultArray[(10 * i) + 8] = 1e-100;
                    }
                    wordspairs.put(pair, defaultArray);
				}
			}
			reader.close();
            Configuration conf = context.getConfiguration();
            Path outputPath = new Path(conf.get("CSV.output.path"));
            FileSystem fs = outputPath.getFileSystem(conf);
            csvOut = fs.create(new Path(outputPath, "word_similarity.csv"));              

            // Write CSV header
            writeCSVHeader(csvOut);
		}

        private void writeCSVHeader(FSDataOutputStream out) throws IOException {
            // Define a StringBuilder for better performance
            StringBuilder header = new StringBuilder();
        
            // First column: word pairs
            header.append("word1,word2");
        
            // Feature columns
            for (int i = 0; i < 24; i++) {
                header.append(",feature").append(i);
            }
        
            // Class label column
            header.append(",class\n");  // New line at the end
        
            // Write to file
            out.writeBytes(header.toString());
        }

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: w1 w2 label feature     val: w p1 p2 p3 p4
			String[] keyParts = key.toString().split(Env.SPACE);
            String w1 = keyParts[0];
            String w2 = keyParts[1];
            wordPairs pair = new wordPairs(w1, w2, keyParts[2]);
            double[] w1Probs = new double[4];
            double[] w2Probs = new double[4];
            
            for (Text val : values) {
                String[] value = val.toString().split(Env.SPACE);
                double[] measures = {
                                     Double.parseDouble(value[1]), 
                                     Double.parseDouble(value[2]), 
                                     Double.parseDouble(value[3]), 
                                     Double.parseDouble(value[4])};
                if(w1.equals(value[0])){
                    w1Probs = measures.clone();
                } else if (w2.equals(value[0])) {
                    w2Probs = measures.clone();
                }
            }
            // Perform calculations for equations 9, 10, 11, 13, 15, and 17
            if (wordspairs.containsKey(pair)){
                for(int i = 0 ; i < 4; i++){
                    wordspairs.get(pair)[(10*i)+0] += calculateManhattanDistance(w1Probs, w2Probs, i);

                    wordspairs.get(pair)[(10*i)+1] += calculateEuclideanDistance(w1Probs, w2Probs, i); // [Math.sqrt(val)]

                    // Three cells for calculateCosineSimilarity
                        // wordspairs.get(pair)[i+2] -> dotProduct
                        // wordspairs.get(pair)[i+3] -> normL1
                        // wordspairs.get(pair)[i+4] -> normL2
                    calculateCosineSimilarity(w1Probs, w2Probs, pair, i); // [dotProduct / (Math.sqrt(normL1) * Math.sqrt(normL2))] 

                    // Two cells for calculateJaccardSimilarity
                        // wordspairs.get(pair)[i+5] -> min_sum
                        // wordspairs.get(pair)[i+6] -> max_sum
                    calculateJaccardSimilarity(w1Probs, w2Probs, pair, i); // [min_sum / max_sum]

                    // Two cells for calculateDiceSimilarity
                        // wordspairs.get(pair)[i+7] -> numenator
                        // wordspairs.get(pair)[i+8] -> denumerator
                    calculateDiceSimilarity(w1Probs, w2Probs, pair, i); // [2 * numenator / denumerator]

                    wordspairs.get(pair)[(10*i)+9] += calculateJensenShannonDivergence(w1Probs, w2Probs, i);
                }
            }
		}

        private String arrayToString (double[] arr){
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < arr.length; i++) {
                result.append(arr[i]).append(",");
            }
            return result.toString();
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            for (Map.Entry<wordPairs, double[]> entry : wordspairs.entrySet()) { // Iterate over all wordspairs
                wordPairs pair = entry.getKey();
                double[] originalArray = entry.getValue();
                double[] newArray = new double[24];
        
                for (int i = 0; i < 4; i++) {
                    newArray[(6 * i) + 0] = originalArray[(10 * i) + 0];
                    newArray[(6 * i) + 1] = Math.sqrt(originalArray[(10 * i) + 1]);
                    newArray[(6 * i) + 2] = (originalArray[(10 * i) + 3] == 0 || originalArray[(10 * i) + 4] == 0)
                        ? 0.0
                        : originalArray[(10 * i) + 2] / (Math.sqrt(originalArray[(10 * i) + 3]) * Math.sqrt(originalArray[(10 * i) + 4]));
        
                    newArray[(6 * i) + 3] = (originalArray[(10 * i) + 6] == 0)
                        ? 0.0
                        : originalArray[(6 * i) + 5] / originalArray[(10 * i) + 6];
        
                    newArray[(6 * i) + 4] = (originalArray[(10 * i) + 8] == 0)
                        ? 0.0
                        : 2 * originalArray[(10 * i) + 7] / originalArray[(10 * i) + 8];
        
                    newArray[(6 * i) + 5] = originalArray[(10 * i) + 9];
                }

                newValue.set(arrayToString(wordspairs.get(pair))); //TODO - needed????
                newKey.set(pair.toString());
                context.write(newKey, newValue);
        
                String[] parts = pair.toString().split(Env.space);
                if (parts.length != 3) {
                    continue;
                }
        
                String word1 = parts[0];
                String word2 = parts[1];
                String label = parts[2];
        
                String data = Arrays.stream(newArray).mapToObj(String::valueOf).collect(Collectors.joining(","));
                if (csvOut != null) {
                    csvOut.writeBytes(word1 + "," + word2 + "," + data + "," + label + "\n");
                }
            }
        
            if (csvOut != null) {
                csvOut.close();
            }
        }

        // Example methods for calculations
        //Equation 9
        private double calculateManhattanDistance(double[] w1Probs, double[] w2Probs, int i) {
            return Math.abs(w1Probs[i] - w2Probs[i]);
        }

        //Equation 10
        private double calculateEuclideanDistance(double[] w1Probs, double[] w2Probs, int i) {
            if (w1Probs[i] != 0 && w2Probs[i] != 0){
                return Math.pow(w1Probs[i] - w2Probs[i], 2);
            }
            if (w1Probs[i] != 0){
                return Math.pow(w1Probs[i], 2);
            }
            if (w2Probs[i] != 0){
                return Math.pow(w2Probs[i], 2);
            }
            return 0;
        }

        //Equation 11
        private void calculateCosineSimilarity(double[] w1Probs, double[] w2Probs, wordPairs pair, int i) {
            if (w1Probs[i] != 0 && w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+2] += w1Probs[i] * w2Probs[i];
                wordspairs.get(pair)[(10*i)+3] += Math.pow(w1Probs[i], 2);
                wordspairs.get(pair)[(10*i)+4] += Math.pow(w2Probs[i], 2);
            }
            else if (w1Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+3] += Math.pow(w1Probs[i], 2);
            }
            else if (w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+4] += Math.pow(w2Probs[i], 2);
            }
            return;
        }

        //Equation 13
        private void calculateJaccardSimilarity(double[] w1Probs, double[] w2Probs, wordPairs pair, int i) {
            if (w1Probs[i] != 0 && w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+5] += Math.min(w1Probs[i], w2Probs[i]);
                wordspairs.get(pair)[(10*i)+6] += Math.max(w1Probs[i], w2Probs[i]);
            }
            else if (w1Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+5] += Math.min(w1Probs[i], 0.0);
                wordspairs.get(pair)[(10*i)+6] += Math.max(w1Probs[i], 0.0);
            }
            else if (w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+5] += Math.min(0.0, w2Probs[i]);
                wordspairs.get(pair)[(10*i)+6] += Math.max(0.0, w2Probs[i]);
            }
            return;
        }

        //Equation 15
        private void calculateDiceSimilarity(double[] w1Probs, double[] w2Probs, wordPairs pair, int i) {
            if (w1Probs[i] != 0 && w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+7] += Math.min(w1Probs[i], w2Probs[i]);
                wordspairs.get(pair)[(10*i)+8] += (w1Probs[i] + w2Probs[i]);
            }
            else if (w1Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+8] += w1Probs[i];
            }
            else if (w2Probs[i] != 0){
                wordspairs.get(pair)[(10*i)+8] += w2Probs[i];
            }
            return;
        }

        //Equation 17
        private double calculateJensenShannonDivergence(double[] w1Probs, double[] w2Probs, int i){
            // Calculate midpoint distribution M = (P+Q)/2
            double midpoint = (w1Probs[i] + w2Probs[i])/2;
            
            // Calculate divargence = KL(P||M) + KL(Q||M)
            double divergence = 0.0;
            if (w1Probs[i] != 0){
                divergence += w1Probs[i] * Math.log(w1Probs[i] / midpoint);
            }
            if (w2Probs[i] != 0){
                divergence += w2Probs[i] * Math.log(w2Probs[i] / midpoint);
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
        Path outputPath = new Path(args[2]);
        conf.set("CSV.output.path", outputPath.toString());
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
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		boolean jobSuccess = job.waitForCompletion(true);
        if (jobSuccess) {
            System.out.println("MapReduce job completed. Running classifier...");

            String jarKey = "classifier.jar";
            String localJarPath = "/tmp/classifier.jar";
            AWS aws = AWS.getInstance();
            String downloadedJarPath = aws.downloadFromS3(Env.PROJECT_NAME, jarKey, "/tmp");

            if (downloadedJarPath == null) {
                System.err.println("Failed to download classifier JAR from S3.");
                System.exit(1);
            }

            System.out.println("Successfully downloaded classifier.jar to " + localJarPath);

            String CSVPathStr = args[2]; 

            ProcessBuilder processBuilder = new ProcessBuilder("java", "-jar", localJarPath, CSVPathStr);
            processBuilder.inheritIO(); 
            Process process = processBuilder.start();
            process.waitFor();
        }
        System.exit(jobSuccess ? 0 : 1);
	}
}