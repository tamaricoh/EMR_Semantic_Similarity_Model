package dsp3;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class sequenceToArffStep { //TODO - check all paths!
    
    public static class sequenceToArffMapper 
        extends Mapper<Text, ArrayWritable, Text, Text> {
        
        private FSDataOutputStream arffOut;
        
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path outputPath = new Path(conf.get("arff.output.path"));
            FileSystem fs = outputPath.getFileSystem(conf);
            arffOut = fs.create(new Path(outputPath, "word_similarity.arff"));
            
            // Write ARFF header
            writeArffHeader(arffOut);
        }
        
        private void writeArffHeader(FSDataOutputStream out) throws IOException {
            out.writeBytes("@relation word_pairs\n\n");
            out.writeBytes("@attribute word1_word2 string\n");
            
            for (int i = 0; i < 24; i++) {
                out.writeBytes("@attribute feature" + i + " numeric\n");
            }
            
            out.writeBytes("@attribute class {0,1}\n\n");
            out.writeBytes("@data\n");
        }
        
        @Override
        public void map(Text key, ArrayWritable value, Context context) 
            throws IOException, InterruptedException {
            
            String[] parts = key.toString().split(" ");
            if (parts.length != 3) {
                return;
            }
            
            String word1 = parts[0];
            String word2 = parts[1];
            String label = parts[2];
            
            Writable[] features = value.get();
            StringBuilder featureString = new StringBuilder();
            
            for (int i = 0; i < features.length; i++) {
                if (i > 0) featureString.append(",");
                featureString.append(((DoubleWritable)features[i]).get());
            }
            
            // Write directly to ARFF file
            arffOut.writeBytes(word1 + "_" + word2 + "," + featureString.toString() + "," + label + "\n");
        }
        
        @Override
        protected void cleanup(Context context) throws IOException {
            if (arffOut != null) {
                arffOut.close();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SequenceToArff <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sequence to arff");
        job.setJarByClass(sequenceToArffStep.class);
        
        job.setMapperClass(sequenceToArffMapper.class);
        job.setNumReduceTasks(0);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        Path outputPath = new Path(args[1]);
        conf.set("arff.output.path", outputPath.toString());
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        
        if (job.waitForCompletion(true)) {
            AWS aws = AWS.getInstance();
            aws.uploadFileToS3(outputPath + "/word_similarity.arff", Env.PROJECT_NAME);
            
            String jarPath = Env.S3_BUCKET_PATH + "/WordSimilarityClassifier.jar";
            ProcessBuilder pb = new ProcessBuilder("java", "-jar", jarPath, Env.S3_BUCKET_PATH); //TODO - args?
            Process process = pb.start();
            process.waitFor();
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}