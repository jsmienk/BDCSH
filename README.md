# Big Data Computing & Storage with Hadoop

## Assignments

>After you have completed lessons 1 through 7 of the Udacity Course, mentioned in the introduction the following assignments have to be done:

1. Written in Python (2 weeks).
2. Written in Java (0.5 week).
3. Written in Java (0.5 week).

### 1.1 Music Streaming

>The `songplayhistory.zip` which contains 3 files in which the listening history of users of a Spotify-like channel are included:
>
>- `people.csv`
>- `tracks.csv`
>- `playhistory.csv`
>
>Write mappers and reducers to solve the following four problems (1.1.1, 1.1.2, 1.1.3 and 1.1.4):
>
>You should hand in the source code of the mappers and the reducers and a (small) report in which you explain your solution and display the results of your solution for the [large dataset](https://leren.saxion.nl/bbcswebdav/pid-2157184-dt-content-rid-51191183_4/xid-51191183_4).

#### 1.1.1 Play Count per Song per Month

>For each song how often was listened to that song in a certain month of a particular year,
i.e. March 2015. Expected output: (SongId, number of times played in March 2015), ordered by SongID.

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
        date = datetime.strptime(date_string.strip(), '%Y-%m-%d').strftime('%Y %m')

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

Running a Hadoop job on the large data set resulted in the following output:

```text
...
# Track id            Date       Listen count
TREX0CN128F92F8F89    2016 01    7
TREX0CN128F92F8F89    2016 02    6
TREX0CN128F92F8F89    2016 03    5
TREX0CN128F92F8F89    2016 04    3
TREX0CN128F92F8F89    2016 05    3
TREX0CN128F92F8F89    2016 06    2
TREX0CN128F92F8F89    2016 07    4
TREX0CN128F92F8F89    2016 08    9
TREX0CN128F92F8F89    2016 09    6
TREX0CN128F92F8F89    2016 10    5
TREX0CN128F92F8F89    2016 11    3
TREX0CN128F92F8F89    2016 12    4
TREX0CN128F92F8F89    2017 01    2
TREX0CN128F92F8F89    2017 02    7
TREX0CN128F92F8F89    2017 03    5
TREX0CN128F92F8F89    2017 04    4
TREX0CN128F92F8F89    2017 05    5
TREX0CN128F92F8F89    2017 06    5
TREX0CN128F92F8F89    2017 07    10
TREX0CN128F92F8F89    2017 08    8
TREX0CN128F92F8F89    2017 09    2
TREX0CN128F92F8F89    2017 10    6
TREX0CN128F92F8F89    2017 11    5
TREX0CN128F92F8F89    2017 12    3
TREX0CN128F92F8F89    2018 01    4
TREX0CN128F92F8F89    2018 02    4
TREX0CN128F92F8F89    2018 03    11
TREX0CN128F92F8F89    2018 04    5
TREX0CN128F92F8F89    2018 05    7
TREX0CN128F92F8F89    2018 06    3
TREX0CN128F92F8F89    2018 07    2
TREX0CN128F92F8F89    2018 08    4
...
```

#### 1.1.2 Songs Listened to per User per Hour of the Day

>For each user the hour of the day (s)he listened most often to songs. Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song in that hour of the day).

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
...
# Name             Hour  Listen count
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
# Title                            Artist              Listen count
Come Out Your Frame (Interlude)    Styles of Beyond    29
To                                 Zao                 28
Sweetest Love                      Dean Frazer         27
Ballast                            Jawbox              27
Necesito Respirar                  Medina Azahara      27
```

#### 1.1.4 Favourite Artist per User

>For each user, the artist (s)he listen to most often. Expected output: (FirstName, LastName, Artist, NrofTimes listened to that artist) (Hint: you need a cascade of mappers and reducers. Explain why!).

combine people and playhistory to get everything the user listened to.
mapper prints user_id, (first_name, last_name), (track_id)
reducer prints user_id, first_name, last_name, track_id, listen_count

combine result round 1 with tracks to find the artists for every song the user listened to.
mapper prints track_id, (artist), (user_id, first_name, last_name, listen_count)
reducer prints track_id, artist, user_id, first_name, last_name, listen_count

reorder to calc user stats
mapper prints user_id, first_name, last_name, track_id, artist, listen_count
reducer prints first_name, last_name, top_artist, listen_count

Running a Hadoop job on the large data set resulted in the following output:

First round results:

```python
...
# User  Name   Track id           Listen count
10,Rhys,Pearne,TRDGUYP128F92DCE2C,2
10,Rhys,Pearne,TRCNUPQ128F933EBBC,1
10,Rhys,Pearne,TRECGFS128F9300400,1
10,Rhys,Pearne,TREKCYB128F14A9880,1
10,Rhys,Pearne,TRBKNXM128F930E3F6,1
10,Rhys,Pearne,TRCNUSH128F4247622,1
10,Rhys,Pearne,TRENJBP128E0791D68,1
10,Rhys,Pearne,TREJUTA128F92E95C2,1
10,Rhys,Pearne,TRBENMR12903CE9B16,1
10,Rhys,Pearne,TRCOBDF128F426324F,1
10,Rhys,Pearne,TRBEKVF128F42306E1,1
...
```

Second round mapper output:

```python
# Hypothetical
TRATSJR128EF34E1F3,AC/DC ,- ,-      ,-     ,0
TRATSJR128EF34E1F3,-     ,43,Marnick,Arend ,3
TRATSJR128EF34E1F3,-     ,22,Jeroen ,Smienk,4
TRATXQY128F1489B0C,-     ,22,Jeroen ,Smienk,1
TRATXQY128F1489B0C,Krezip,- ,-      ,-     ,0
TRATXQY128F1489B0C,-     ,43,Marnick,Arend ,2
```

Second round results:

```python
...
# Track id         Artist         User Name                Listen count
TRATSCZ12903CDAF86,Edge of Sanity,1333,Kristo   ,O Cannan ,1
TRATSCZ12903CDAF86,Edge of Sanity,135 ,Jilleen  ,Tyhurst  ,1
TRATSCZ12903CDAF86,Edge of Sanity,1355,Rickard  ,Cardenoza,1
TRATSCZ12903CDAF86,Edge of Sanity,1359,Oralia   ,Gwillym  ,1
TRATSCZ12903CDAF86,Edge of Sanity,1369,Effie    ,Cariss   ,1
TRATSCZ12903CDAF86,Edge of Sanity,1374,Claudell ,Shedd    ,1
TRATSCZ12903CDAF86,Edge of Sanity,1398,Ingelbert,Kalberer ,1
TRATSCZ12903CDAF86,Edge of Sanity,144 ,Donavon  ,Cudiff   ,1
...
```

Third round mapper output:

```python
...

...
```

Third round (final) results:

```python
...
# Name                Artist               Listen count
Kissie Pymar          Groove Collective    3
Silvio Manzell        iiO                  4
Dennet Chicchelli     Rebeca               4
Stormi Beltzner       Johnny Gill          4
Madison Houldin       The Four Aces        4
Arte Siss             Right Said Fred      3
Clarance Braybrookes  Nnenna Freelon       3
Timofei Tute          Smokey Robinson & the Miracles    4
Vasily Vidloc         Scott Joplin         5
Livvy Symonds         Mickey Gilley        3
Danell Karpe          Horace Silver        4
Livvie Thatcher       Anathallo            4
Blondelle McGurgan    Suzy Bogguss         3
Nolana Swains         Machine Head         5
Tris Georgeot         Jorge Ben Jor        3
Thomasine Whitfeld    Jimmy Ruffin         3
Bernetta Hutson       Bruce Dickinson      4
...
```

### 1.2 Shakespeare

>For this assignment are given are works by Shakespeare, recorded in the `InvertedIndexInput.tgz` file. If you unpack this file you will have a directory with all the works of Shakespeare.
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

#### 1.2 Result

### 1.3 Web Log

>In this assignment we take the accesslog data file from the Udacity course of assignment 1.1 as a starting point, in which it is registered which IP addresses have access on a website:
>
>Program the following map-reduce programs in Java:
>
>1. A program that determines for each IP address how often a hit is administered.
>2. We want to have an overview per month of the year that states per IP address, how often that particular month the website was visited from that IP address. Think of a solution that can help you achieve this.
>
>Hint: Think of a solution where you have 12 reducers and make sure that every reducer handles all hits of one specific month. To do this you must define a partitioner.

#### 1.3 Result

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

##### 7. Result

```text
fantastic      345  [...]
fantastically  4    [17583, 1007765, 1025821, 7004477, 9006895]
```

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

##### 9. Result

Mean sales for every day of the week. Weekdays are represented as integers (Monday equals 0).

```text
0    250.009331149
1    249.738227929
2    249.851167195
3    249.872024327
4    250.223089314
5    250.084703253
6    249.946443251
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

## Technical Help & Commands

CentOS 6 VM git fix:

```text
sudo yum update -y nss curl libcurl
sudo yum install git
```

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

`hadoop fs -get <dirname> (outputfile)` to get files from HDFS.

`hadoop fs -put <filename> (path)` to move files to HDFS.

`hadoop fs -mv <oldfile> <newfile>` to rename files in HDFS.

`hadoop fs -rmdir <dirname>` to remove old output directories.

### Running jobs

`hadoop jar usr/lib/hadoop-mapreduce/hadoop-streaming.jar -mapper mapper.py -reducer reducer.py -input <filename> -output <dirname>` to run a full Hadoop job on an input file that is in HDFS.

`hs mapper.py reducer.py <inputfile> <outputdir>` to run a full Hadoop job on an input file that is in HDFS.