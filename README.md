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
    LISTEN_COUNT = 1

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

        print('{0},{1},{2}'.format(track_id, date, LISTEN_COUNT))
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
        final Text work = new Text(((FileSplit) context.getInputSplit()).getPath().getName());
        // Get the whole line
        final String line = value.toString();

        // Save the line number for all words on this line
        IntWritable lineNumber = null;
        // For every word in this line
        for (final String word : line.split("\\W+")) {
            // If line number is null (first hit)
            if (lineNumber == null) {
                // Check if it is the line number
                try {
                    lineNumber = new IntWritable(Integer.parseInt(word));
                } catch (NumberFormatException nfe) {
                    // First word is not a line number!
                    System.out.println(line);
                    System.out.println(nfe.getMessage());
                    // Skip this whole line
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

Writable custom class used as a value between te Mapper and Reducer. It implements `Writable` which required it to override `write(DataOutput)` and `readFields(DataInput)` for serialization. A custom `toString()` method is provided to help the Reducer print the output of this value.

```java
class InvertedIndex implements Writable {

    private Text work;
    private IntWritable line;

    // Public empty constructor required for serialization
    public InvertedIndex() {}

    InvertedIndex(final Text work, final IntWritable line) {
        this.work = work;
        this.line = line;
    }

    /**
     * Custom toString method to help printing the output in the reducer
     */
    @Override
    public String toString() {
        return work.toString() + '@' + line.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(line.get());
        out.writeUTF(work.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read in same order as write
        line = new IntWritable(in.readInt());
        work = new Text(in.readUTF());
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
>Hint: Think of a solution where you have 12 reducers and make sure that every reducer handles all hits of one specific month. To do this you must define a partitioner.

#### 1.3.1 Total Hits per IP

>A program that determines for each IP address how often a hit is administered.

##### 1.3.1 Mapper

##### 1.3.1 Reducer

##### 1.3.1 Result

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

##### 1.3.2 Mapper

##### 1.3.2 Reducer

##### 1.3.2 Result

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
fantastically  5    [17583, 1007765, 1025821, 7004477, 9006895]
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

### Python

#### Mapper.py

```python
#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():
        # CODE HERE

mapper()
```

#### Reducer.py

```python
#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():
        # CODE HERE

reducer()
```

#### Testing MapReduce with Python

`head -50 <inputfile> > <outputfile>` to create a test file of 50 lines.

`cat <testfile> | ./mapper.py | sort` to test the mapper alone.

`cat <testfile> | ./mapper.py | sort | ./reducer.py` to test the Hadoop workflow.

Make sure to give `mapper.py` and `reducer.py` to correct file permissions by running `chmod +x <filename>`.

#### Hadoop Streaming with Python

`hadoop jar usr/lib/hadoop-mapreduce/hadoop-streaming.jar -mapper mapper.py -reducer reducer.py -input <filename> -output <dirname>` to run a full Hadoop job on an input file that is in HDFS.

`hs mapper.py reducer.py <input> <outputdir>` to run a full Hadoop job on an input file that is in HDFS.

### Java

Add the following libraries to your IDE:

- [Hadoop HDFS](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs).
- [Hadoop Common](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common).
- [Hadoop Core](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core).

#### Driver.java

Code that runs on the client to configure and
submit the job.

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

#### Mapper.java

```java
public class MyMapper extends Mapper<LongWritable, Text, MyWritableKey, MyWritableValue> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new MyWritableKey(), new MyWritableValue())
    }
}
```

#### Reducer.java

```java
public class MyReducer extends Reducer<MyWritableKey, MyWritableValue, MyOtherWritableKey, MyOtherWritableValue> {

    public void reduce(MyWritableKey key, Iterable<MyWritableValue> values, Context context) throws IOException, InterruptedException {
        context.write(new MyOtherWritableKey(), new MyOtherWritableValue());
    }
}
```

#### Hadoop Streaming with Java

`javac -classpath 'hadoop classpath' *.java` to compile class files.

`jar cvf <name>.jar *.class` to create a .jar file using the compiled class files.

`hadoop jar <name>.jar <main class name> <input> <outputdir>` to run a full Hadoop job on an input file that is in HDFS.

### Hadoop Distributed File System (HDFS)

#### Exploring

`hadoop fs -ls (path)` to show the current files in the path.

`hadoop fs -get <dirname> (outputfile)` to get files from HDFS.

`hadoop fs -put <filename> (path)` to move files to HDFS.

`hadoop fs -mv <oldfile> <newfile>` to rename files in HDFS.

`hadoop fs -rmdir <dirname>` to remove old output directories.