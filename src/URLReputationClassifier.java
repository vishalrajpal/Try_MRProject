
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class URLReputationClassifier
{
   private static List<Double> oldWeightVector;
   private static double oldBias;
   
   public static List<Double> getOldWeightVector() {
      return oldWeightVector;
   }

   public static double getOldBias() {
      return oldBias;
   }
   
   public static void main(String[] args) {
      List<String> lines;
      try
      {
         int noOfWords = 3231961;
         oldWeightVector = new ArrayList<Double>(Collections.nCopies(noOfWords, (double)0.0));
         oldBias = 0.0;
         for(int i = 1; i<=10; i++){
            Configuration conf = new Configuration();
            Job job = new Job(conf, "URLReputation");
            job.setJarByClass(URLReputationClassifier.class);
            job.setMapperClass(URLReputationMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(DoubleArrayWritable.class);
            job.setReducerClass(URLReputationReducer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(DoubleArrayWritable.class);

            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]+i));
            if(!job.waitForCompletion(true)) {
               System.exit(1);
            } else {
               String updatedVectorsFileName = args[2]+""+i+"/part-r-00000";
               FileReader reader = new FileReader(new File(updatedVectorsFileName));
               BufferedReader brReader = new BufferedReader(reader);
               String line = brReader.readLine();
               String[] split = line.split("\\t");
               oldBias = Double.parseDouble(split[0]);
               split = split[1].split(" ");
               int splitLen = split.length;
               double val;
               for(int counter = 0; counter<splitLen; counter++) {
                  System.out.print(split[counter]+" ");
                  oldWeightVector.set(counter, oldWeightVector.get(counter) + Double.parseDouble(split[counter]));
               }
               System.out.println();
               brReader.close();
            }

         }
      }
      catch (Exception e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
}
