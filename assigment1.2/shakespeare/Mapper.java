static class Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        final String line = value.toString();
        boolean foundKey = false;
        for (final String word : line.split("\\W+")) {
            if (!foundKey) {

            }
            if (word.length() > 0) {
                context.write(new Text(word), ONE);
            }
        }
    }
}