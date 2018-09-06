# Big Data Computing & Storage with Hadoop

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

> Make sure to create a case-insensitive index (e.g. "FANTASTIC" and "fantastic" should both count towards the same word).
>
> You can download the additional dataset [here](http://content.udacity-data.com/course/hadoop/forum_data.tar.gz). To unarchive it, download it to your VM, put in the data directory and run:

```bash
tar zxvf forum_data.tar.gz
```