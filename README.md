# Big Data Computing & Storage with Hadoop

CentOS 6 VM update git fix [here](https://stackoverflow.com/questions/21820715/how-to-install-latest-version-of-git-on-centos-7-x-6-x).

CentOS 6 VM git fix: `sudo yum update -y nss curl libcurl`

## Writing MapReduce in Python

### Mapper

```python
#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():
    # CODE HERE

mapper()
```

### Reducer

```python
#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():
    # CODE HERE

reducer()
```

### Testing MapReduce in CentOS 7

`head -50 <inputfile> > <outputfile>` to create a test file of 50 lines.

`cat <testfile> | ./mapper.py | sort` to test the mapper alone.

`cat <testfile> | ./mapper.py | sort | ./reducer.py` to test the Hadoop workflow.

Make sure to give `mapper.py` and `reducer.py` to correct file permissions by running `chmod +x <filename>`.

## Hadoop Distributed File System (HDFS)

### Exploring

`hadoop fs -ls (path)` to show the current files in the path.

`hadoop fs -get <dirname> <outputfile>` to get files from HDFS.

`hadoop fs -put <filename>` to move files to HDFS.

`hadoop fs -rmdir <dirname>` to remove old output directories.

### Running jobs

`hadoop jar usr/lib/hadoop-mapreduce/hadoop-streaming.jar -mapper mapper.py -reducer reducer.py -input <filename> -output <dirname>` to run a full Hadoop job on an input file that is in HDFS.

`hs mapper.py reducer.py <inputfile> <outputdir>` to run a full Hadoop job on an input file that is in HDFS.

## Assignments

The course containes three assignments:

1. Written in Python (2 weeks).
2. Written in Java.
3. Written in Java.

### 1.1

>After you have completed lessons 1 through 7 of the Udacity Course, mentioned in the introduction the following assignments have to be done:
>
>The `songplayhistory.zip` which contains 3 files in which the listening history of users of a Spotify-like channel are included:
>
>- `people.csv`
>- `tracks.csv`
>- `playhistory.csv`
>
>Write mappers and reducers to solve the following problems:
>
>1. For each song how often was listened to that song in a certain month of a particular year,
i.e. March 2015. Expected output: (SongId, number of times played in March 2015), ordered by SongID.
>2. For each user the hour of the day (s)he listened most often to songs. Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song.
in that hour of the day)
>3. The 5 songs played most often in a specific hour of the day i.e. between 7AM and 8AM. Expected output: 5 lines containing (Songtitle, ArtistName, NumberOfTimesPlayed).
>4. For each user, the artist (s)he listen to most often. Expected output: (FirstName, LastName, Artist, NrofTimes listened to that artist) (Hint: you need a cascade of mappers and reducers. Explain why!).
>
>You should hand in the source code of the mappers and the reducers and a (small) report in which you explain your solution and display the results of your solution for the large dataset.

### 1.2

>For this assignment are given are works by Shakespeare, recorded in the `InvertedIndexInput.tgz` file. If you unpack this file you will have a directory with all the works of Shakespeare.
>
>Each file contains a work by Shakespeare in the following format:
>
>```text
>0  HAMLET
>1
>2
>3  DRAMATIS PERSONAE 4
>5
>6  CL AUDIUS king of Denmark. (KING CL AUDIUS:)
>7
>8  HAMLET son to the late, and nephew to the present king. 9
>10
>11 POLONIUS lord chamberlain. (LORD POLONIUS:)
>12 ...
>```
>
>Each line contains:
>
>- A line number
>- Separation character: tab character
>- Value: a line of text
>
>This format can be read directly by using the `KeyValueTextInputFormat` class, which is available in the Hadoop API. This input format offers each line as 1 record for your mapper, using the section for the tab as key and the part after the tab as value.
>
>It is asked to write a map-reducing program in Java that produces a cross-reference, also called inverted index, of all words in the works of Shakespeare, based on the text as given in the above form. In this cross-reference, every word that occurs in Shakespeare's works is recorded on which line(s) in a work by Shakespeare that word can be found.
>
>For example, for the word 'honeysuckle' the output looks like this:
>
>`honeysuckle    2kinghenryiv@1038, midsummernightsdream@2175, ...`
>
>This indicates that the word honeysuckle occurs in the work '2kinghentyiv' on line 1038 and in the work 'midsummernightsdream' on line 2175
>
>Hint: To get the name of the input file that is processed at a certain moment, the following statement can be used:
`String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();`

### 1.3

>In this assignment we take the accesslog data file from the Udacity course of assignment 1.1 as a starting point, in which it is registered which IP addresses have access on a website:
>
>Program the following map-reduce programs in Java:
>
>1. A program that determines for each IP address how often a hit is administered.
>2. We want to have an overview per month of the year that states per IP address, how often that particular month the website was visited from that IP address. Think of a solution that can help you achieve this.
>
>Hint: Think of a solution where you have 12 reducers and make sure that every reducer handles all hits of one specific month. To do this you must define a partitioner.

## Udacity Course

### Lesson 7

#### 7. Inverted Index

>Write a MapReduce program that creates an index of all words that can found in the body of a forum post and node id they can be found in.
>
>Do not parse the HTML. Split the text on special charachters: . , ! ? : ; " ' ( ) < > { } [ ] # $ = - /.
>
>- How many times was the word "fantastic" used in forums?
>- List of comma separated nodes the word "fantastically" can be found in.
>
>Make sure to create a case-insensitive index (e.g. "FANTASTIC" and "fantastic" should both count towards the same word).
>
>You can download the additional dataset [here](http://content.udacity-data.com/course/hadoop/forum_data.tar.gz). To unarchive it, download it to your VM, put in the data directory and run:
>
>```bash
>tar zxvf forum_data.tar.gz
>```

##### Result

fantastic       345 [...]

fantastically   4   [17583, 1007765, 1025821, 7004477, 9006895]

#### 9. Finding Mean

>Write a MapReduce program that processes the `purchases.txt` file and outputs mean (averages) of sales for each workday.
>
>You can find the weekday by the date by using builtin Python functionality:
>
>```python
>from datetime import datetime
>
>weekday = datetime.strptime(date, "%Y-%m-%d").weekday()
>```
>
>You can download the additional dataset [here](http://content.udacity-data.com/courses/ud617/purchases.txt.gz). Change the file name to purchases.gz.gz to succefully unzip on CentOS 7.

##### Result

Mean sales for every day of the week. Weekdays are represented as integers (Monday equals 0).

```text
0 250.009331149
1 249.738227929
2 249.851167195
3 249.872024327
4 250.223089314
5 250.084703253
6 249.946443251
```

#### 10. Combiners

>To use combiner, you will have to add a new shortcut command to your VM. In the terminal window type
>
>`gedit ~/.bashrc`
>
>In the editor that opens, find a function definition `run_mapreduce`. Copy the contents and create a new function (within the same file), let's say `run_mapreduce_with_combiner`. Add the following `-combiner $2` right after `-reducer $2`.
>
>And at the end of the file, add a line for the alias:
>
>`alias hsc=run_mapreduce_with_combiner`
>
>(or whatever you called that function). You can also change the alias itself, just make sure you are not trying to use any already existing Linux command names.
>
>Now save the file and exit the gedit program. Run the following in the terminal:
>
>`source ~/.bashrc`
>
>This will reload the configuration file you just edited, and your new alias should be ready to use.
>
>The new alias will take the second parameter (which is the reducer script) and also use it for combiner. If you want, you can actually make another alias, that allows you to use a different script for combiner. You would need to also upload it, same as you did for mapper and reducer scripts.
