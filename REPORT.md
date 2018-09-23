# Big Data Computing & Storage with Hadoop 1

*Big Data Assignments in Python and Java, 23 sept. 2018*

**Authors: Marnick van der Arend 415010, Jeroen Smienk 422516, EHI4VBDa.**

**Lecturer: Jan Stroet**

## Table of Contents

- [Introduction](#introduction)
- [Preparation](#preparation)
  - [Python](#python)
    - [Testing & Streaming](#testing--streaming)
  - [Java](#java)
    - [Testing & Streaming](#testing--streaming-1)
  - [Hadoop Distributed File System](#hadoop-distributed-file-system)
- [Assignments](#assignments)
  - [1.1 Music Streaming](#11-music-streaming)
    - [1.1.1 Play Count per Song per Month](#111-play-count-per-song-per-month)
    - [1.1.2 Songs Listened to per User per Hour of the Day](#112-songs-listened-to-per-user-per-hour-of-the-day)
    - [1.1.3 Top 5 Songs Played at Hour of the Day](#113-top-5-songs-played-at-hour-of-the-day)
    - [1.1.4 Favourite Artist per User](#114-favourite-artist-per-user)
  - [1.2 Shakespeare](#12-shakespeare)
  - [1.3 Web Log](#13-web-log)
    - [1.3.1 Total Hits per IP](#131-total-hits-per-ip)
    - [1.3.2 Total IP Hits per Month](#132-total-ip-hits-per-month)
- [Problems & Solutions](#problems--solutions)

## Introduction

// TODO

## Preparation

We were provided with a CentOS Virtual Machine (VM) image that had Hadoop installed. After updating Git we were able to write code on our own machines and pull and test it on the VM.

### Python

This was our first experience with the Python programming language. It is pretty straightforward, but some research had to be done considering the format of the file to be able to make it work.

Input for both the mapper and reducer comes from 'standard input' (`sys.in`).

An empty mapper script in Python would look like this:

```python
#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():
    for line in sys.in:

mapper()
```

An empty reducer script in Python would look like this:

```python
#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():
    for line in sys.in:

reducer()
```

Except for naming, they are identical.

#### Testing & Streaming

In order to test a mapper and/or reducer on a smaller sample of the dataset without having to run a full Hadoop job we used the following commands:

- `head -<N> <inputfile> > <outputfile>` to create a test file of N lines.
- `cat <testfile> | ./mapper.py | sort` to test the mapper alone.
- `cat <testfile> | ./mapper.py | sort | ./reducer.py` to test the Hadoop workflow.

To run the mapper and reducer Python scripts, its file permissions had to be explicitly changed using: `chmod +x <filename>`.

To run a full Hadoop job the following command and its shortcut exist:

- `hadoop jar usr/lib/hadoop-mapreduce/hadoop-streaming.jar -mapper mapper.py -reducer reducer.py -input <filename> -output <dirname>` (full)
- `hs mapper.py reducer.py <input> <outputdir>` (shortcut)

### Java

In order to develop a solution on our own machines without any errors the following libraries have to be added to the project. This allows us to build our code and spot compile errors before trying to create and run a JAR file and the VM.

- [Hadoop HDFS](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs).
- [Hadoop Common](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common).
- [Hadoop Core](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core).

Because Java is a very different programming language than Python, it also requires a somewhat different file structure. One big difference is the need for a 'driver'. The driver configures, submits and controls our job and its mapper(s) and reducer(s).

_Hadoop takes care of providing the input and output files as arguments to the main method._

```java
public class Driver {

    public static void main(String[] args) throws Exception {
        final Job job = new Job();
        job.setJarByClass(Driver.class);
        job.setJobName("JobName");

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(MyWritableKey.class);
        job.setOutputValueClass(MyWritableValue.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

The mapper class extends from `Mapper` from one of the Hadoop libraries. It requires us to fill in four generics. The input key and value and the output key and value. The default input key and value for the mapper are `LongWritable` and `Text`. The libraries use their own boxed variants of basic data types for serialization. You can also write your own by implementing `Writable`.

_You choose the output key and value yourself. The input key and value for the mapper are in our assignments always the default. This is dependent on the source files._

One more important difference compared to using Python is that instead of iterating over `sys.in` the `map(Key, Value, Context)` method is called once for every line in the source file(s).

```java
public class MyMapper extends Mapper<LongWritable, Text, MyWritableKey, MyWritableValue> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new MyWritableKey(), new MyWritableValue())
    }
}
```

The reducer class extends from `Reducer` from one of the Hadoop libraries. It also requires us to fill in four 'key value' generics. The input key and value should of course be of the same data type as the output key and value of its corresponding mapper. Its output key and value depend on the problem you are solving.

Like the mapper we are not iterating over `sys.in`. Instead the `reduce(Key, Iterable<Value>, Context)` method is called once for every key the reducer receives. This is an even bigger difference than Python, because we get all the values for one key at once.

```java
public class MyReducer extends Reducer<MyWritableKey, MyWritableValue, MyOtherWritableKey, MyOtherWritableValue> {

    public void reduce(MyWritableKey key, Iterable<MyWritableValue> values, Context context) throws IOException, InterruptedException {
        context.write(new MyOtherWritableKey(), new MyOtherWritableValue());
    }
}
```

#### Testing & Streaming

We did not find a quicker and easier way to test 'Java Hadoop' with smaller sample datasets than running a full Hadoop job.

- `javac -classpath 'hadoop classpath' *.java` to compile class files.
- `jar cvf <name>.jar *.class` to create a .jar file using the compiled class files.
- `hadoop jar <name>.jar <main class name> <input> <outputdir>` to run a full Hadoop job on an input file that is in HDFS.

### Hadoop Distributed File System

We are of course using Hadoop Distributed File System (HDFS) on the VM and this requires us to interact with it in specific ways. All the commands we needed to use Hadoop were:

- `hadoop fs -ls (path)` to show the current files in the path.
- `hadoop fs -get <dirname> (outputfile)` to get files from HDFS.
- `hadoop fs -put <filename> (path)` to move files to HDFS.
- `hadoop fs -mv <oldfile> <newfile>` to rename or move files in HDFS.
- `hadoop fs -rm -r <dirname>` to remove old output directories.

## Assignments

The first three assignments of this course are covered in this report:

1. Music Streaming written in Python (2 weeks).
2. Shakespeare written in Java (0.5 week).
3. Web Log written in Java (0.5 week).

Per (sub)assignment we will explain how our code works and why we choose to use certain solutions.

### 1.1 Music Streaming

This first assignment consists of four subassignments. Each one increases in difficulty. Three `.csv` files are provided in both small and very large sizes to use for testing and the full Hadoop job.

>The `songplayhistory.zip` which contains 3 files in which the listening history of users of a Spotify-like channel are included:
>
>- `people.csv`
>- `tracks.csv`
>- `playhistory.csv`
>
>Write mappers and reducers to solve the following four problems (1.1.1, 1.1.2, 1.1.3 and 1.1.4):

Each problem will be individually discussed in the next sections.

#### 1.1.1 Play Count per Song per Month

>For each song how often was listened to that song in a certain month of a particular year,
i.e. March 2015. Expected output: (SongId, number of times played in March 2015), ordered by SongID.

The assignment is to find the total play count for every song per month. We worked by first creating psuedo code as comments for both the mapper and reducer. We both individually worked out one of either scripts.

##### 1.1.1 Mapper

Our mapper checks if the amount of columns is sufficient and skips the header of the `.csv`. We use the track id as the `key` to pass on to the reducer, because we want to know how often each song (track id) is listened to. It then gets the year and month from the timestamp and uses that as the first value. We provide `1` as the second value, because that is how often this song was listened to on that date.

```python
def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')
        if len(data) < 3:
            continue

        track_id, user_id, date_string = data
        # Skip header line
        if track_id == 'track_id':
            continue

        # Reformat date to lose useless time
        date = datetime.strptime(date_string.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y %m')

        print('{0},{1},{2}'.format(track_id, date, 1))
```

##### 1.1.1 Reducer

The reducer includes two functions. The reducer itself and a helper for printing the output. We keep track of three variables outside the for-loop: `prev_track`, `curr_track` and `curr_track_plays`. The `prev_track` is the track id of previous track we have been visiting. The `curr_track` is current track we are looking at in the for-loop. `curr_track_plays` is a dictionary containing key-value pairs being date and total play count for each song.

After splitting the input line and checking its column count we check if the third parameter (play count) is an integer. We check if we arrived at a new track (not the very first). If this is true, we print the results of that previous track we finished visiting and clear the dictionary. If this track is the first track we are visiting or the same as the previous track, we check if the dictionary key for that date is initialized and increase its value with the play count.

When the for-loop ends, we print the final track's result. This has to be done here, because there is no next track to trigger the printing of the previous track's result inside the for-loop.

Printing the result is done with a helper function. It loopes through all sorted keys of the `curr_track_plays` dictionary and prints the track id, date and total play count for every entry. We sort the keys so not only the track ids are sorted in the output but also the dates per track id.

```python
def reducer():
    prev_track = None
    curr_track = None
    curr_track_plays = {}

    for line in sys.stdin:
        data = line.strip().split(',')
        # Check argument count
        if len(data) != 3:
            continue

        # Check argument type
        if not data[2].isdigit():
            continue

        curr_track = data[0]
        date = data[1]
        count = int(data[2])

        # If current word does not equal previous word
        if prev_track and prev_track != curr_track:

            # Print results
            print_result(prev_track, curr_track_plays)

            # Reset dict
            curr_track_plays.clear()

        # Increase count per month
        if not curr_track_plays.has_key(date):
            curr_track_plays[date] = 0

        curr_track_plays[date] = curr_track_plays[date] + count

        # Set the current track as the previous track for next iteration
        prev_track = curr_track

    # Print the last track and its count
    print_result(prev_track, curr_track_plays)

# Print the previous track and its count per month
def print_result(track_id, dict):
    for month in sorted(dict.keys()):
        print("{0}\t{1}\t{2}".format(track_id, month, dict[month]))
```

##### 1.1.1 Results

Running a Hadoop job on the large data set resulted in the following output:

```text
# Track id            Date       Listen count
TRATSCZ12903CDAF86    2014 01    5
TRATSCZ12903CDAF86    2014 02    3
TRATSCZ12903CDAF86    2014 03    7
TRATSCZ12903CDAF86    2014 04    2
TRATSCZ12903CDAF86    2014 05    5
TRATSCZ12903CDAF86    2014 06    9
TRATSCZ12903CDAF86    2014 07    3
TRATSCZ12903CDAF86    2014 08    3
TRATSCZ12903CDAF86    2014 09    7
TRATSCZ12903CDAF86    2014 10    2
TRATSCZ12903CDAF86    2014 11    4
TRATSCZ12903CDAF86    2014 12    4
TRATSCZ12903CDAF86    2015 01    13
TRATSCZ12903CDAF86    2015 02    2
TRATSCZ12903CDAF86    2015 03    5
TRATSCZ12903CDAF86    2015 04    5
TRATSCZ12903CDAF86    2015 05    5
TRATSCZ12903CDAF86    2015 06    3
TRATSCZ12903CDAF86    2015 07    8
TRATSCZ12903CDAF86    2015 08    1
TRATSCZ12903CDAF86    2015 09    6
TRATSCZ12903CDAF86    2015 10    3
TRATSCZ12903CDAF86    2015 11    4
TRATSCZ12903CDAF86    2015 12    3
TRATSCZ12903CDAF86    2016 01    6
TRATSCZ12903CDAF86    2016 02    8
TRATSCZ12903CDAF86    2016 03    3
TRATSCZ12903CDAF86    2016 04    4
TRATSCZ12903CDAF86    2016 05    5
TRATSCZ12903CDAF86    2016 06    3
TRATSCZ12903CDAF86    2016 07    5
TRATSCZ12903CDAF86    2016 08    2
TRATSCZ12903CDAF86    2016 09    4
TRATSCZ12903CDAF86    2016 10    8
TRATSCZ12903CDAF86    2016 11    7
TRATSCZ12903CDAF86    2016 12    10
TRATSCZ12903CDAF86    2017 01    8
TRATSCZ12903CDAF86    2017 02    5
TRATSCZ12903CDAF86    2017 03    7
TRATSCZ12903CDAF86    2017 04    5
TRATSCZ12903CDAF86    2017 05    7
TRATSCZ12903CDAF86    2017 06    8
TRATSCZ12903CDAF86    2017 07    6
TRATSCZ12903CDAF86    2017 08    5
TRATSCZ12903CDAF86    2017 09    2
TRATSCZ12903CDAF86    2017 10    7
TRATSCZ12903CDAF86    2017 11    3
TRATSCZ12903CDAF86    2017 12    7
TRATSCZ12903CDAF86    2018 01    7
TRATSCZ12903CDAF86    2018 02    7
TRATSCZ12903CDAF86    2018 03    5
TRATSCZ12903CDAF86    2018 04    6
TRATSCZ12903CDAF86    2018 05    9
TRATSCZ12903CDAF86    2018 06    6
TRATSCZ12903CDAF86    2018 07    5
TRATSCZ12903CDAF86    2018 08    8
...
```

#### 1.1.2 Songs Listened to per User per Hour of the Day

>For each user the hour of the day (s)he listened most often to songs. Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song in that hour of the day).

For this assignment we need to combine data from two different sources for the first time. The way this works using Python is to feed both sources to the mapper after each other. Determining which line is from which source can be done by comparing the amount of columns in the CSV file.

##### 1.1.2 Mapper

The mapper's task is to combine data from two different sources. One being `playhistory.csv` and the other being `people.csv`. We need to combine the full name of the user with the amount of plays per hour of the day. The mapper does not know if it gets input from `playhistory.csv` or `people.csv`, but we can determine this by looking at the amount of columns. If the columns do not match either file, we skip that line. It is important that we have atleast one common value to be able to combine the data. In this case this common value is `user_id`. This column is present in both source files.

We get the useful data from the lines and always print it in the same format so our reducer can expect what his input will be. Depending on the source of the current line that is being processed some values may be zero or not applicable ('-'). The only value that must always be present is the key. The key is always the common value used to combine the data from the different sources. In our case `user_id`.

```python
# Output line with data from people.csv
22,Jeroen,Smienk,-,0

# Output line with data from playhistory.csv
22,-,-,7,1

# In both cases the id is the same, so we can match the play count with the user's name.
```

```python
def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        hour_of_day = '-'
        listened_count = 0

        if len(data) == 3: # playhistory.csv
            user_id = data[1]
            time_stamp = data[2]
            hour_of_day = datetime.strptime(time_stamp.strip(), '%Y-%m-%d %H:%M:%S').hour
            listened_count = 1
        elif len(data) == 7: # people.csv
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]
        else: # invalid source
            continue

        # Skip header line / invalid sources
        if not user_id.isdigit():
            continue

        print('{0},{1},{2},{3},{4}'.format(user_id, first_name, last_name, hour_of_day, listened_count))
```

##### 1.1.2 Reducer

The reducer includes two functions. The reducer itself and a helper for printing the output. We keep track of five variables outside the for-loop: `prev_user`, `curr_user` and `curr_user_playhistory`. The `prev_user` is the user id of previous user we have been visiting. The `curr_user` is current user we are looking at in the for-loop. `curr_user_playhistory` is a list with the index being hour of the day and its value the total play count for each user.

This looks a lot like the previous assignment's reducer, but because we have to combine data now as well it looks a little different. When there is a first and last name present in the current line, we save it. After counting all other lines with the same user id. We print an output line and reset the name and play history variables.

Input data (prepared by the mapper) may look like this:

```python
# Hypothetical results
...
43,-      ,-     ,14,1
43,-      ,-     ,6 ,1
43,Marnick,Arend ,- ,0
43,-      ,-     ,15,1
43,-      ,-     ,16,1
43,-      ,-     ,6 ,1
22,Jeroen ,Smienk,- ,0
22,-      ,-     ,9 ,1
22,-      ,-     ,7 ,1
22,-      ,-     ,12,1
22,-      ,-     ,7 ,1
...
```

The lines are sorted on the key (user id) so we can be certain that all play counts and eventually the name will be together. When a new user id is visited (`prev_user != curr_user`) the all play counts and the name can be printed as a single row. No need to sort the list on hour here like we did in the last assignment, because we used the indices of the array as the hours of the day.

```python
def reducer():
    prev_user = None
    curr_user = None
    curr_user_playhistory = [0] * 24
    curr_user_first_name = None
    curr_user_last_name = None

    for line in sys.stdin:
        data = line.strip().split(',')

        # Check argument count
        if len(data) != 5:
            continue

        curr_user, first_name, last_name, hour_of_day, listened_count = data

        # Check argument type
        if not listened_count.isdigit():
            continue

        # If current user_id does not equal previous user_id
        if prev_user and prev_user != curr_user:

            # Print the previous user's playhistory
            print_result(curr_user_first_name, curr_user_last_name, curr_user_playhistory)

            # Reset variables
            curr_user_first_name = None
            curr_user_last_name = None
            curr_user_playhistory = [0] * 24

        if curr_user_first_name == None and first_name != '-':
            curr_user_first_name = first_name

        if curr_user_last_name == None and last_name != '-':
            curr_user_last_name = last_name

        if hour_of_day.isdigit():  
            hour_of_day = int(hour_of_day)
            # Increase listen count
            curr_user_playhistory[hour_of_day] += int(listened_count)

        # Set the current user_id as the previous user_id for next iteration
        prev_user = curr_user

    # Print the current user's playhistory
    print_result(curr_user_first_name, curr_user_last_name, curr_user_playhistory)

def print_result(first_name, last_name, list):
    for hour, count in enumerate(list):
        print("{0}\t{1}\t{2}\t{3}".format(first_name, last_name, hour, count))
```

Running a Hadoop job on the large data set resulted in the following output:

```python
# Name             Hour  Listen count
...
Kimberley Fredy    0     29
Kimberley Fredy    1     24
Kimberley Fredy    2     26
Kimberley Fredy    3     22
Kimberley Fredy    4     33
Kimberley Fredy    5     18
Kimberley Fredy    6     28
Kimberley Fredy    7     28
Kimberley Fredy    8     33
Kimberley Fredy    9     24
Kimberley Fredy    10    27
Kimberley Fredy    11    27
Kimberley Fredy    12    30
Kimberley Fredy    13    27
Kimberley Fredy    14    34
Kimberley Fredy    15    29
Kimberley Fredy    16    38
Kimberley Fredy    17    27
Kimberley Fredy    18    29
Kimberley Fredy    19    23
Kimberley Fredy    20    28
Kimberley Fredy    21    21
Kimberley Fredy    22    25
Kimberley Fredy    23    22
Rhys Pearne        0     27
Rhys Pearne        1     24
Rhys Pearne        2     38
Rhys Pearne        3     30
Rhys Pearne        4     31
Rhys Pearne        5     32
Rhys Pearne        6     18
Rhys Pearne        7     25
Rhys Pearne        8     25
Rhys Pearne        9     23
Rhys Pearne        10    24
Rhys Pearne        11    27
Rhys Pearne        12    23
Rhys Pearne        13    24
Rhys Pearne        14    22
Rhys Pearne        15    34
Rhys Pearne        16    22
Rhys Pearne        17    34
Rhys Pearne        18    13
Rhys Pearne        19    27
Rhys Pearne        20    23
Rhys Pearne        21    27
Rhys Pearne        22    19
Rhys Pearne        23    35
...
```

#### 1.1.3 Top 5 Songs Played at Hour of the Day

>The 5 songs played most often in a specific hour of the day i.e. between 7AM and 8AM. Expected output: 5 lines containing (Songtitle, ArtistName, NumberOfTimesPlayed).

We choose to find songs between 12:00 and 13:00.

Running a Hadoop job on the large data set resulted in the following output:

```python
# Title                   Artist                                Listen count
Think About (The Beach)   Clubraiders                           32
Blue Moon                 I Giganti                             28
Battle of the Species     Antibalas Afrobeat Orchestra          26
I Know                    Dr. Ring-Ding & The Senior Allstars   26
Me culpas de todo         Medina Azahara                        26
```

#### 1.1.4 Favourite Artist per User

>For each user, the artist (s)he listen to most often. Expected output: (FirstName, LastName, Artist, NrofTimes listened to that artist) (Hint: you need a cascade of mappers and reducers. Explain why!).

Combining play history and artists.

Combining play history and names.

Reducing names and artists to the most favourite artist per user.

##### 1.1.4 Mapper 1

```python
def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')
        # Skip header line
        if data[0] == 'track_id':
            continue

        track_id = None
        artist = '-'
        user_id = '-'
        listen_count = 0

        if len(data) == 3: # playhistory.csv
            track_id = data[0]
            user_id = data[1]
            listen_count = 1

            if not user_id.isdigit():
                continue
        elif len(data) >= 4: # tracks.csv
            track_id = data[0]
            artist = data[1]
            # title not important
        else:
            continue

        print('{0},{1},{2},{3}'.format(track_id, artist, user_id, listen_count))
```

##### 1.1.4 Reducer 1

```python
def reducer():
    prev_track = None
    curr_track = None
    prev_artist = None
    prev_track_listeners = []

    for line in sys.stdin:
        data = line.strip().split(',')

        if len(data) != 4:
            continue

        curr_track, curr_artist, user_id, listen_count = data

        # Check argument type
        if not listen_count.isdigit():
            continue

        # If current track_id does not equal previous track_id
        if prev_track and prev_track != curr_track:

            # Print the previous track's listeners
            print_result(prev_track, prev_artist, prev_track_listeners)

            # Reset variables
            prev_artist = None
            prev_track_listeners = []

        # Save the artist of the current track_id
        if curr_artist != '-':
            prev_artist = curr_artist

        # Append a user to the listeners list
        if user_id != '-':
            prev_track_listeners.append((user_id, listen_count))

        # Set the current track_id as the previous track_id for next iteration
        prev_track = curr_track

    # Print the current track's listeners
    print_result(prev_track, prev_artist, prev_track_listeners)

# Print track_id, artist, user_id, listen_count
def print_result(track_id, artist, listeners):
    for listener in listeners:
        print("{0},{1},{2},{3}".format(track_id, artist, listener[0], listener[1]))
```

##### 1.1.4 Mapper 2

```python
def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        artist = '-'
        listen_count = 0

        if len(data) == 4: # output from the first mapper
            track_id = data[0]
            artist = data[1]
            user_id = data[2]
            listen_count = data[3]

            if not user_id.isdigit() or not listen_count.isdigit():
                continue
        elif len(data) == 7: # people.csv
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]

            # Skip header line
            if user_id == 'id':
                continue
        else:
            continue

        print('{0},{1},{2},{3},{4}'.format(user_id, first_name, last_name, artist, listen_count))
```

##### 1.1.4 Reducer 2

```python
def reducer():
    prev_user = None
    curr_user = None
    prev_first_name = None
    prev_last_name = None
    prev_user_artists = {}

    for line in sys.stdin:
        data = line.strip().split(',')
        if len(data) != 5:
            continue

        curr_user, first_name, last_name, artist, listen_count = data

        # If current user does not equal previous user
        if prev_user and prev_user != curr_user:

            # Print results
            print_result(prev_first_name, prev_last_name, prev_user_artists)

            # Reset vars
            prev_first_name = None
            prev_last_name = None
            prev_user_artists.clear()

        # First name visited
        if prev_first_name == None and first_name != '-':
            prev_first_name = first_name
        # Last name visited
        if prev_last_name == None and last_name != '-':
            prev_last_name = last_name

        # Increase count per artist if line contains an artist
        if artist:
            if not prev_user_artists.has_key(artist):
                prev_user_artists[artist] = 0

            prev_user_artists[artist] += int(listen_count)

        # Set the current user as the previous user for next iteration
        prev_user = curr_user

    # Print the last user and its favourite artist
    print_result(prev_first_name, prev_last_name, prev_user_artists)

# Print the previous user and its favourite artist
def print_result(first_name, last_name, dict):
    favouriteTuple = sorted(dict.items(), key=lambda x:x[1], reverse=True)[0]
    print("{0} {1}\t{2}\t{3}".format(first_name, last_name, favouriteTuple[0], favouriteTuple[1]))
```

Running a Hadoop job on the large data set resulted in the following output:

First round results:

```python
# Track id         Artist         User id   Listen count
TRATSCZ12903CDAF86,Edge of Sanity,1107,1
TRATSCZ12903CDAF86,Edge of Sanity,1124,1
TRATSCZ12903CDAF86,Edge of Sanity,1148,1
TRATSCZ12903CDAF86,Edge of Sanity,1167,1
TRATSCZ12903CDAF86,Edge of Sanity,1169,1
TRATSCZ12903CDAF86,Edge of Sanity,1300,1
TRATSCZ12903CDAF86,Edge of Sanity,1305,1
TRATSCZ12903CDAF86,Edge of Sanity,1371,1
TRATSCZ12903CDAF86,Edge of Sanity,1402,1
TRATSCZ12903CDAF86,Edge of Sanity,1408,1
TRATSCZ12903CDAF86,Edge of Sanity,1410,1
TRATSCZ12903CDAF86,Edge of Sanity,1443,1
TRATSCZ12903CDAF86,Edge of Sanity,1450,1
TRATSCZ12903CDAF86,Edge of Sanity,1454,1
TRATSCZ12903CDAF86,Edge of Sanity,1462,1
TRATSCZ12903CDAF86,Edge of Sanity,1531,1
TRATSCZ12903CDAF86,Edge of Sanity,1538,1
...
```

Second round results:

```python
# Name                    Artist                    Listen count
Kimberley Fredy           Jimmy Somerville          3
Rhys Pearne               Sepultura                 3
Kimberly Bartlomieczak    Alan Parsons              3
Bald Shrubshall           Pet Shop Boys             4
Othilia Denness           Charles Mingus            4
Mandi Yanin               The Andrews Sisters       6
Tan Cullip                Mantovani                 4
Claudianus Woolger        Burl Ives                 4
Sandra Doddridge          Pet Shop Boys             4
Hakim Chainey             Queen                     3
Simone Barehead           Edith Márquez             3
Xaviera Jacquest          The Golden Gate Quartet   3
May Filipczak             Absolute                  3
Margarethe Waleworke      Rush                      3
Fabian Seagar             Career Suicide            4
Dene Jacobsohn            Howlin Wolf               4
Maddie Lavies             Luna                      3
Evie Bogart               Howlin Wolf               4
Neila McCorkell           Jim Lauderdale            4
Titos Hordell             Ironik                    4
...
```

### 1.2 Shakespeare

>For this assignment are given are works by Shakespeare, recorded in the [`InvertedIndexInput.tgz`](https://leren.saxion.nl/bbcswebdav/pid-2157184-dt-content-rid-50887938_4/xid-50887938_4) file. If you unpack this file you will have a directory with all the works of Shakespeare.
>
>Each file contains a work by Shakespeare in the following format:
>
>```text
>0    HAMLET
>1
>2
>3    DRAMATIS PERSONAE 4
>5
>6    CL AUDIUS king of Denmark. (KING CL AUDIUS:)
>7
>8    HAMLET son to the late, and nephew to the present king. 9
>10
>11   POLONIUS lord chamberlain. (LORD POLONIUS:)
>12   ...
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
>`honeysuckle        2kinghenryiv@1038, midsummernightsdream@2175, ...`
>
>This indicates that the word honeysuckle occurs in the work '2kinghentyiv' on line 1038 and in the work 'midsummernightsdream' on line 2175
>
>Hint: To get the name of the input file that is processed at a certain moment, the following statement can be used:
`String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();`

#### 1.2 Mapper

```java
class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, InvertedIndex> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the name of the file
        final String work = ((FileSplit) context.getInputSplit()).getPath().getName();
        // Save the line number for all words on this line
        int lineNumber = -1;
        // For every word in this line
        for (final String word : value.toString().split("\\W+")) {
            // If line number is null (first hit)
            if (lineNumber == -1) {
                // Check if it is the line number
                try {
                    lineNumber = Integer.parseInt(word);
                } catch (NumberFormatException ignored) {
                    // Skip line
                    break;
                }
            } else if (word.length() > 0) {
                // Write every word with its work and line number
                context.write(new Text(word.toLowerCase()), new InvertedIndex(work, lineNumber));
            }
        }
    }
}
```

##### InvertedIndex.java

Writable custom class used as a value between te mapper and reducer. It implements `Writable` which required it to override `write(DataOutput)` and `readFields(DataInput)` for serialization. A custom `toString()` method is provided to help the reducer print the output of this value.

```java
class InvertedIndex implements Writable {
    private String work;
    private int line;

    // Public empty constructor required for serialization
    public InvertedIndex() {}

    InvertedIndex(final String work, final int line) {
        this.work = work;
        this.line = line;
    }

    /**
     * Custom toString method to help printing the output in the reducer
     */
    @Override
    public String toString() {
        return work + '@' + line;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(line);
        out.writeUTF(work);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read in same order as write
        line = in.readInt();
        work = in.readUTF();
    }
}
```

#### 1.2 Reducer

```java
class InvertedIndexReducer extends Reducer<Text, InvertedIndex, Text, Text> {

    public void reduce(Text key, Iterable<InvertedIndex> values, Context context) throws IOException, InterruptedException {
        final StringBuilder builder = new StringBuilder();
        // For every inverted index of the same word
        for (final InvertedIndex val : values) {
            // Append it as 'work@line'
            builder.append(val.toString());
            // Not the last one?
            if (values.iterator().hasNext()) {
                // Separate with commas
                builder.append(", ");
            }
        }
        // Write the word with all its indices as a single line
        context.write(key, new Text(builder.toString()));
    }
}
```

#### 1.2 Result

Running a full Hadoop job results in the following output:

```java
...
youth       muchadoaboutnothing@3262, muchadoaboutnothing@710, muchadoaboutnothing@3591, muchadoaboutnothing@1894, muchadoaboutnothing@2602, muchadoaboutnothing@1601, muchadoaboutnothing@712, macbeth@2071, coriolanus@822, coriolanus@612, hamlet@1900, hamlet@3644, hamlet@1453, ...
youths      macbeth@3324, troilusandcressida@3610, periclesprinceoftyre@2492, juliuscaesar@1145, kinghenryviii@4490
zanies      twelfthnight@657
zany        loveslabourslost@3609, glossary@2427
zeal        troilusandcressida@3531, troilusandcressida@3676, 2kinghenryiv@3143, 2kinghenryiv@2073, 2kinghenryiv@4699, titusandronicus@684, merchantofvenice@3745, timonofathens@1809, timonofathens@3369, twogentlemenofverona@1416, kingrichardii@157, kingrichardii@3606, ...
zealous     kingrichardiii@3440, allswellthatendswell@2315, kingjohn@474, kingjohn@1004, loveslabourslost@3024, sonnets@465
zeals       timonofathens@746
zed         kinglear@1843
zenelophon  loveslabourslost@1674
zenith      tempest@468
zephyrs     cymbeline@3615
zir         kinglear@4502, kinglear@4489
zo          kinglear@4495
zodiac      titusandronicus@802
zodiacs     measureforemeasure@446
zone        hamlet@5374
zounds      1kinghenryiv@1271, 1kinghenryiv@1020, 1kinghenryiv@671, 1kinghenryiv@4370, 1kinghenryiv@348, 1kinghenryiv@1765, 1kinghenryiv@4321, 1kinghenryiv@3149, 1kinghenryiv@1153, 1kinghenryiv@1631, romeoandjuliet@2423, romeoandjuliet@2336, kingrichardiii@1462, ...
zwaggered   kinglear@4494
```

### 1.3 Web Log

>In this assignment we take the [accesslog data file](https://leren.saxion.nl/bbcswebdav/pid-2157184-dt-content-rid-50887939_4/xid-50887939_4) from the Udacity course of assignment 1.1 as a starting point, in which it is registered which IP addresses have access on a website:
>
>Program the following map-reduce programs in Java:
>
>1. A program that determines for each IP address how often a hit is administered.
>2. We want to have an overview per month of the year that states per IP address, how often that particular month the website was visited from that IP address. Think of a solution that can help you achieve this.
>
> Hint: Think of a solution where you have 12 reducers and make sure that every reducer handles all hits of one specific month. To do this you must define a partitioner.

Every line in the input file named `access_log` consists of an IP address, a date and the request details.

e.g.: `10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET / HTTP/1.1" 403 202`.

#### 1.3.1 Total Hits per IP

>A program that determines for each IP address how often a hit is administered.

##### 1.3.1 Mapper

```java
class IPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split on whitespace
        final String[] partials = value.toString().split("\\s+");

        if (partials.length > 0) {
            // Write output
            context.write(new Text(partials[0]), new IntWritable(1));
        }
    }
}
```

##### 1.3.1 Reducer

The reducer calculates the sum of an IP address' hits.

```java
class IPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        while (values.iterator().hasNext()) {
            count += values.iterator().next().get();
        }
        context.write(key, new IntWritable(count));
    }
}
```

##### 1.3.1 Result

Running a full Hadoop job results in the following output:

```text
10.1.1.113      1
10.1.1.125      12
10.1.1.144      1
10.1.1.195      4
10.1.1.236      12
10.1.1.5        1
10.1.10.155     2
10.1.10.197     2
10.1.10.198     1
10.1.10.48      1
10.1.10.5       1
10.1.100.104    1
10.1.100.13     1
10.1.100.138    1
10.1.100.183    14
10.1.100.199    35
10.1.100.5      1
10.1.101.135    29
10.1.101.140    1
10.1.101.141    1
...
```

#### 1.3.2 Total IP Hits per Month

>We want to have an overview per month of the year that states per IP address, how often that particular month the website was visited from that IP address. Think of a solution that can help you achieve this.
>
>Hint: Think of a solution where you have 12 reducers and make sure that every reducer handles all hits of one specific month. To do this you must define a partitioner.

One difference in the driver of this solution is that we use one reducer for every month of the year. We set this by using: `job.setNumReduceTasks(12);`.

##### 1.3.2 Mapper

```java
class MonthMapper extends Mapper<LongWritable, Text, IntWritable, IPOccurrence> {

    private static final SimpleDateFormat SDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
    private static final Calendar CAL = Calendar.getInstance();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split on whitespace
        final String[] partials = value.toString().split("\\s+");

        // Check argument count
        if (partials.length > 3) {
            // Use the IP address as the value
            final IPOccurrence ipCount = new IPOccurrence(partials[0], 1);
            // Cutting the '[' of the front
            final String dateString = partials[3].substring(1);

            // Try to parse the date and extract the month
            try {
                // Use month as key
                CAL.setTime(SDF.parse(dateString));
                final IntWritable month = new IntWritable(CAL.get(Calendar.MONTH));
                // Write output
                context.write(month, ipCount);
            } catch (ParseException ignored) {}
        }
    }
}
```

###### IPOccurrence.java

Writable custom class used as a value between te mapper and reducer. It implements `Writable` which required it to override `write(DataOutput)` and `readFields(DataInput)` for serialization. A custom `toString()` method is provided to help the reducer print the output of this value.

```java
class IPOccurrence implements Writable {
    private String ip;
    private int count;

    public IPOccurrence() {}

    IPOccurrence(final String ip, final int count) {
        this.ip = ip;
        this.count = count;
    }

    String getIp() { return ip; }

    int getCount() { return count; }

    @Override
    public String toString() {
        return ip + "\t" + count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeUTF(ip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        ip = in.readUTF();
    }
}
```

##### 1.3.2 Partitioner

```java
public class MonthPartitioner extends Partitioner<IntWritable, IPOccurrence> {
    public int getPartition(final IntWritable key, final IPOccurrence _, final int numReduceTasks) {
        return key.get() % numReduceTasks; // Get the remainder to safely pass the key,value on.
    }
}
```

##### 1.3.2 Reducer

```java
class MonthReducer extends Reducer<IntWritable, IPOccurrence, IntWritable, IPOccurrence> {

    public void reduce(IntWritable month, Iterable<IPOccurrence> values, Context context) throws IOException, InterruptedException {
        // For every month create dictionary of ip's and count ip address
        final Iterator<IPOccurrence> it = values.iterator();
        final Map<String, Integer> map = new HashMap<>();

        while (it.hasNext()) {
            final IPOccurrence occurrence = it.next();
            final String ipAddress = occurrence.getIp();
            if (!map.containsKey(ipAddress)) {
                map.put(ipAddress, 0);
            }
            map.put(ipAddress, map.get(ipAddress) + occurrence.getCount());
        }

        // Sort on IP address and output total hits per IP for this reducer's month
        final SortedSet<String> ipAddresses = new TreeSet<>(map.keySet());
        for (final String ipAddress : ipAddresses) {
            context.write(month, new IPOccurrence(ipAddress, map.get(ipAddress)));
        }
    }
}
```

##### 1.3.2 Result

Running a full Hadoop job results in the following output:

_Month integers are zero indexed._

**May**:

```text
4    10.1.1.125      12
4    10.1.100.13     1
4    10.1.100.138    1
4    10.1.101.92     1
4    10.1.11.81      69
4    10.1.111.244    1
4    10.1.112.22     23
4    10.1.12.172     54
4    10.1.120.14     14
4    10.1.122.3      2
4    10.1.123.140    1
4    10.1.125.31     1
4    10.1.126.88     21
4    10.1.13.175     3
4    10.1.131.59     1
...
```

**December**:

```text
11    10.1.115.106    1
11    10.1.115.114    1
11    10.1.116.146    1
11    10.1.12.137     1
11    10.1.120.14     19
11    10.1.123.207    1
11    10.1.131.59     1
11    10.1.133.149    1
11    10.1.138.236    1
11    10.1.14.196     1
11    10.1.141.120    2
11    10.1.143.54     2
11    10.1.144.192    1
11    10.1.15.51      1
11    10.1.161.229    1
...
```

## Problems & Solutions

In the beginning it was not entirely clear how to structure and edit the Python script files to make them run properly. We figured out we had to give the file executable permissions and that we forgot to call the mapper/reducer function we declared. This was not initially clear from the MOOC.

When we started on the Java portion of the assignments we had no idea what we needed and how the files should look. After searching online, we found which JAR libraries to add and how to develop custom drivers, mappers and reducers.

We wanted to work with Git so we could both develop from our own machines and not in the slow VM. Unfortunately Git and/or one of its dependencies were not up to date on the provided VM image. After some tinkering we got Git to work too.

Testing the Python mapper individually or the whole mapreduce was really easy by simulating the Hadoop workflow in the command line. Unfortunately we did not find such a 'shotcut' when developing in Java. We had to run a full Hadoop job to test the Java code. At least using Java produced error messages when running a Hadoop job. Using Python did not do this.