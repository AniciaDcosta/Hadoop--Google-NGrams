package googlengram.main.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/* This is first job in the first cycle.
 * Here we will be fetching the total occurrence of a particular 1gram and 4grams
 * The data in google Ngrams is not uniformly formatted, some times the 
 * data is separated with one space or more spaces
 * So this Mapper separates splits one gram differently and 4 grams differently
 * this is a simple Mapper class where the 1 gram /4 grams  in each line will be extracted
 * and the occurrence of each  will be added 
 * input = 1gram  year  occurrence  NoOfBooksOccured 
 * 		   4gram  year  occurrence  NoOfBooksOccured 
 * example : dog 1990 10 2
 * 	     dog is faithful animal     1990 40 10
 * output : dog 10
 * 	    dog is faithful animal 40
 * output sample
 * key : 1gram/4gram
 * value : occurence
 * Author: Anicia D'costa
 */

public class NgramCountMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {
	 private Text gram = new Text();
	private LongWritable frequency = new LongWritable();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// converting the text to string
		String word = value.toString();
		// get the name of the file
		FileSplit input_split = (FileSplit) context.getInputSplit();
		Path filePath = input_split.getPath();
		String fileName = filePath.toString();
		String[] s_paths = fileName.split("-");
		// checking if the file is either a 1gram or 4gram
		// file name is of the form : googlebooks-eng-all-1gram-20120701-b
		String fileNgramValue = s_paths[3];
		// variable to store the index
		int tempInt = -1;
		// writing as output if the data is one gram
		if (fileNgramValue.contains("1")) {
			// splitting the data on white spaces
			String[] parts = word.split("\\s+");
			// considering the 1gram only which contains alphabets and
			// underscore
			if ((parts.length == 4) && (parts[0].matches("[a-zA-Z _]+"))) {
				// parsing each 1gram data without underscore
				// example :dog_NOUN would be dog
				if (parts[0].indexOf("_") != -1) {
					tempInt = parts[0].lastIndexOf("_");
					parts[0] = parts[0].substring(0, tempInt);
				}
				// writing as output
				gram.set(parts[0].trim());
				long tempFreq = Long.parseLong(parts[2]);
				frequency.set(tempFreq); 
				context.write(gram , frequency);
			}
		}
		// else breaking down the four grams and writing
		// as output
		else if (fileNgramValue.contains("4")) {
			// $ character is added to differentiate the 4grams from 1
			// grams
			// splitting the data on white spaces
			String[] parts = word.split("\\s+");
			if ((parts.length == 7) && (parts[0].matches("[a-zA-Z _]+")) && (parts[1].matches("[a-zA-Z _]+"))
				 && (parts[2].matches("[a-zA-Z _]+")) && (parts[3].matches("[a-zA-Z _]+"))) {
				// concatenating the four grams together.
				StringBuilder finalKey =  new StringBuilder();
				finalKey.append("$"+ parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
				// considering the 1gram only which contains alphabets and
				// underscore
				gram.set(finalKey.toString());
				long tempFreq = Long.parseLong(parts[5]);
                                frequency.set(tempFreq);
				context.write(gram, frequency);
				
			}
		}

	}

}
