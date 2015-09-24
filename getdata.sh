#!/bin/bash -l

# this script will download google 1gram and 4gram data 
# into the folder /panfs/roc/scratch/teamB/unzippedinput 
# Since we are not using all of the four grams , you could
# randomly choose any 2-4 four grams for each alphabet(data is very huge for all the four grams)
# you should make getdata.sh executable and then run
# from the command line as follows : 
# 
# chmod u+x getdata.sh
# ./getdata.sh

wget -nd -l0 -N -r -A"googlebooks-eng-all-1gram-20120701.*" -P /panfs/roc/scratch/teamB/unzippedInput  http://storage.googleapis.com/books/ngrams/books/datasetsv2.html

wget -nd -l0  -N -r -A"googlebooks-eng-all-4gram-20120701.*" -P /panfs/roc/scratch/teamB/unzippedInput  http://storage.googleapis.com/books/ngrams/books/datasetsv2.html

rm -f wget.log
