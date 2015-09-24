package googlengram.main.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.util.ArrayList;


// This is the Mapper for job 3.
// Author: Anicia Dcosta
// Input: Key: 123234offsetbytes , Value: '1gram#frequency  4gram$4gram$4gram$etc'
// Output: Key: '1gram#frequency' , Value: 'related1gram$1#related1gram$1#related1gram$1'
// Task: It takes in a 1gram (and it's overall frequency) and a string/list of 4grams containing
// The 1gram in question. It then splits up the string/list of 4grams and pulls out
// the words adjacent to the 1gram in question. Each word pulled out this way is given a
// value of '$1' and is append to the string that will be writen out as a value.

public class NgramMapper3
extends Mapper<LongWritable, Text, Text, Text>{   
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//variables
		String[] tempArray;
		int keyLoc = -1;
		int tempInt;

		//Populate a list to check for grammar role so getting rid of articles (EX. We dont want "the" 's so add "the" to list)
		List<String> grammarGate = new ArrayList<String>();
		grammarGate.add("the");
		grammarGate.add("and");
		grammarGate.add("or");
		grammarGate.add("had");
		grammarGate.add("have");
		grammarGate.add("an");
		grammarGate.add("to");
		grammarGate.add("is");
		grammarGate.add("has");
		grammarGate.add("a");
		
		//Splitting the value with "%" which was introduced in the last cycle
		String tempValue = value.toString();
		String parts[] = tempValue.split("%");
		
		//storing a clean key for comparison
		String evalKey = parts[0].toString().trim();
		//Checking for the index of the igram data
		//it is required to find the index of this one gram 
		//data in the 4gram data
		if (evalKey.indexOf("_") != -1) {
			tempInt = evalKey.indexOf("_");
		} else tempInt = evalKey.indexOf("#");
		//getting the exact string for comaprison
		evalKey = evalKey.substring(0, tempInt); 
		//getting rid of the trailing white spaces
		evalKey = evalKey.trim();	
		//all the similar words will be stored in the stringbuilder separated by #
		StringBuilder valueString = new StringBuilder();
		
		//parsing the 4grams
	       //checking for the exact length
		if(parts.length == 2)
		{
			//getting the string of four grams 
			String  tempStr = parts[1].toString();
			//splitting them using "$" as the delimeter
			String[] fourGrams = tempStr.split("\\$");
			
			//march through 4gram array
			for(int i = 0;i < fourGrams.length;i++) {
				//splitting each four gram with white space as delimeter
				tempArray = fourGrams[i].split("\\s+");

				//check for key location for the one gram in the four gram
				for(int a = 0;a < tempArray.length;a++) {
					tempInt = tempArray[a].indexOf("_");
					if(tempInt != -1){
						tempStr = tempArray[a].substring(0, tempInt);
					}
					
					//needs cutting check
					if(evalKey.equals(tempStr.trim()))
					{ 
						keyLoc = a; 
					} 
				}

				//Check to see where key's neighbors are and if they are valid for output
				//here we are forming the co-occurence matrix for each 1 gram data
				// here the window size is 2
				//we will taking the words which are the right and left of the 1 gram data
				if(keyLoc>0){
					if(keyLoc<3) {
						
						//Check against grammarGate, if pass then write out
						tempInt = tempArray[keyLoc-1].indexOf("_"); 
						if(tempInt != -1){
							tempStr = tempArray[keyLoc-1].substring(0,tempInt);

							//needs to be checked for correct cutting
							if(!grammarGate.contains(tempStr)) {
								//tempStr = tempArray[keyLoc-1].substring(0, tempInt);
								//appending to the string builder with count and "#" to seperate from other grams
								valueString.append(tempStr+"$1"+"#");
							}
						}	
						
						//Check against grammarGate, if pass then write out
						tempInt = tempArray[keyLoc+1].indexOf("_");
						if(tempInt != -1){
							tempStr = tempArray[keyLoc+1].substring(0,tempInt);
							//needs to be checked for correct cutting
							if(!grammarGate.contains(tempStr)) {
								//tempStr = tempArray[keyLoc+1].substring(0, tempInt);
								 //appending to the string builder with count and "#" to seperate from other grams
								valueString.append(tempStr+"$1"+"#");
							}
						}
					} else if(keyLoc>=4) {
						//error report : no value was written to keyLoc
					} else {
						
						//Check against grammarGate, if pass then write out
						tempInt = tempArray[keyLoc-1].indexOf("_");
						if(tempInt != -1){
							tempStr = tempArray[keyLoc-1].substring(0,tempInt);
							//needs to be checked for correct cutting
							if(!grammarGate.contains(tempStr)) {
								//tempStr = tempArray[keyLoc-1].substring(0, tempInt);
								//appending to the string builder with count and "#" to seperate from other grams
								valueString.append(tempStr+"$1"+"#");
							}
						}
					}
				} else {
				if (keyLoc < 0) {
						// error report
					} else {
						//Check against grammarGate, if pass then write out
						tempInt = tempArray[keyLoc+1].indexOf("_");
						if(tempInt != -1){
							tempStr = tempArray[keyLoc+1].substring(0,tempInt);
							//needs to be checked for correct cutting
							if(!grammarGate.contains(tempStr)) {
								//tempStr = tempArray[keyLoc+1].substring(0, tempInt);
							 //appending to the string builder with count and "#" to seperate from other grams
								valueString.append(tempStr+"$1"+"#");
							}
						}
					}
				}
		
			}
		}
		//writting the output
		context.write(new Text(parts[0]), new Text(valueString.toString()));
	}
}
