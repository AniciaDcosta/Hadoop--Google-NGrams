package googlengram.main.mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


/* This is the main class of the Google Ngrams - Finding semantic similar words  project
 * The task of this project is to parse through all the 1gram and 4our grams google  data
 * and form clusters of similar words related to eah one gram data
 * 
 * SEE README.pdf IF YOU HAVE NOT
 * 
 *  In this class all the jobs are called in an order
 *  We have designed such that input data is fed once in the first job.
 *  We have 3 cycle of jobs
 *  
 *  
 *  First Cycle:
 *  We are getting the total occurence  of each 1gram/4gram data
 *  We will be taking ony the Ngram with alphabets and "_" character in it.
 *  Input : 1gram/4gram  year  total_no_of_occurences no_of_books_apperances
 *  Output : 1gram/4gram total_noofoccurences
 *  Mapper Class : NgramCountMapper.java
 *  Combiner Class : NgramCountReducer.java
 *  Reducer Class : NgramCountReducer.java
 *  
 *  Example:
 * input = 1gram  year  occurrence  NoOfBooksOccured 
 *         4gram  year  occurrence  NoOfBooksOccured 
 * example : dog 1990 10 2
 *           dog 19991 50 5
 *           dog is faithful animal     1990 40 10
 * output : dog 60
 *          dog is faithful animal 40
 * 
 *
 *  Second Cycle
 * In this cycle we arrange  all the 1grams and related 4grams together
 * This cycle gets rid of the occurences of the 4 grams as we dont not need it
 * Each 1 grams would be stored as the key and all its related four grams would be stored as value
 * Each four garms would be seperated from each other with "$" character 
 * input  sample : dog  40
 *                 $dog is faithful animal 40
 *                 $all love dog animal 60
 *
 * output : dog#40  dog is faithful animal$all love dog animal 60
 * 
 *  Mapper  Class : NgramMapper.java
 *  Reducer  Class : NgramReducer.jav
 *         
 *  Third Cycle
 * This stage aims at calculating the cooccurence matrix and forming the cluster of related words for each 1 gram 
 * Here we split each related four gram , get the exact index of the 1gram within that four grams
 * and look for its left and right neighbors.
 * We use the window size as 2 for forming the co-occurence matrix
 * We only consider words which are either  noun, pronoun, verb.
 * The reducer in this cycle calculates the total occurence of the related gram for the 1 gram
 * Mapper Class: NgramMapper3.java
 * Reducer Class: FinalReducer.java
 * Sample:
 * input: dog#40 %dog animal_NOUN faithful is$all love_VERB dog animal_NOUN 60
 *  
 * output : dog 40  animal 2 faithful 1 love 1
 *   
 *  
 * Author : Anicia Dcosta
 *       
 */ 


         

public class GoogleNgramsMain 
{
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//configuration declaration
		JobConf conf = new JobConf(GoogleNgramsMain.class);		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
		   System.err.println("Usage: googlengrams");
		   System.exit(2);
		}	
	        //creating new job	
		Job job1 = new Job(conf);
		//adding the class
		job1.setJarByClass(GoogleNgramsMain.class);
		//setting the input path
		FileInputFormat.addInputPath(job1,new Path(args[0]));
		//setting the output path
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
                 //setting Mapper class
		job1.setMapperClass(NgramCountMapper.class);
                //setting Reducer class
		job1.setReducerClass(NgramCountReducer.class);
                //setting the combiner class
		job1.setCombinerClass(NgramCountReducer.class);
		job1.setOutputKeyClass(Text.class);
        	job1.setOutputValueClass(LongWritable.class);
       		//setting number of reducers
	        job1.setNumReduceTasks(84);
		//submitting the job
        	job1.waitForCompletion(true); 
			
	        // second cycle
		//creating second job
	        Job job2 = new Job(conf);
		//setting the main class
		 job2.setJarByClass(GoogleNgramsMain.class);
		//adding input path to the second job
		FileInputFormat.addInputPath(job2 ,new Path(args[1]));
		//adding the output path
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		//setting the mapper class
		 job2.setMapperClass(NgramMapper.class);
		//setting the reducer class
		job2.setReducerClass(NgramReducer.class);
		//setting the mapper output value class to text class
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//submitting the job
		job2.setNumReduceTasks(84);
		job2.waitForCompletion(true);

	        // thrid cycle starts here   
                //creating a  new job
            	Job job3 = new Job(conf);
	    	//setting the main class
            	job3.setJarByClass(GoogleNgramsMain.class);
		//setting the mapper class
               job3.setMapperClass(NgramMapper3.class);
		//setting the reducer class
	         job3.setReducerClass(FinalReducer.class);
		//setting the output key class  to text class
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
                job3.setOutputKeyClass(Text.class);
		//setting the output value class to text class
                job3.setOutputValueClass(Text.class);
		//adding input path to the job
		//Complete data will be input data to this job cycle
                FileInputFormat.addInputPath(job3, new Path(args[2]));
		//adding the output path 
            	FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		job3.setNumReduceTasks(26);
		//submitting the job
    	      	job3.waitForCompletion(true);
	     	System.exit(1);


}   

}         
