# Big Data Computing & Storage with Hadoop

CentOS 6 VM update git fix [here](https://stackoverflow.com/questions/21820715/how-to-install-latest-version-of-git-on-centos-7-x-6-x).

CentOS 6 VM git fix: `sudo yum update -y nss curl libcurl`

## Hadoop Distributed File System (HDFS)

### Exploring

`hadoop fs -ls (path)` to show the current files in the path.

`hadoop fs -get <dirname> <outputfile>` to get files from HDFS.

`hadoop fs -put <filename>` to move files to HDFS.

`hadoop fs -rmdir <dirname>` to remove old output directories.

### Running jobs

`hs mapper.py reducer.py <inputfile> <outputdir>`

## Lesson 7

### 7. Quiz Inverted Index

>Write a MapReduce program that creates an index of all words that can found in the body of a forum post and node id they can be found in.
>
>Do not parse the HTML. Split the text on special charachters: . , ! ? : ; " ' ( ) < > { } [ ] # $ = - /.
>
>- How many times was the word "fantastic" used in forums?
>- List of comma separated nodes the word "fantastically" can be found in.
>
> Make sure to create a case-insensitive index (e.g. "FANTASTIC" and "fantastic" should both count towards the same word).
>
> You can download the additional dataset [here](http://content.udacity-data.com/course/hadoop/forum_data.tar.gz). To unarchive it, download it to your VM, put in the data directory and run:
>
>```bash
>tar zxvf forum_data.tar.gz
>```