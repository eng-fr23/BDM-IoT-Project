/*
Dataset: https://www.kaggle.com/sudalairajkumar/covid19-in-usa?select=us_counties_covid19_daily.csv
Query: For each county, return the month which had most cases
*/


package counties_mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class CountyMonths {
    private static final int DATE = 0;
    private static final int COUNTY = 1;
    private static final int MONTH = 1;

    private static final int FMONTH = 0;
    private static final int FCOUNT = 1;


    private static final String JOB_NAME = "Max";

    public static class CountyMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyRes = new Text();
        private Text valRes = new Text();

        private int monthFromDate(String date) {
            return Integer.parseInt(date.split("-")[MONTH]);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valAsString = value.toString();
            String[] valElems = valAsString.split(",");

            int month = monthFromDate(valElems[DATE]);
            String county = valElems[COUNTY];
            String deathCount = valElems[valElems.length - 1];
            String nextVal = String.format("%d,%s", month, deathCount);

            keyRes.set(county);
            valRes.set(nextVal);
            context.write(keyRes, valRes);
        }
    }

    public static class CountyReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable valRes = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int months = 12;
            int[] countByMonth = new int[months];

            for(Text value : values) {
                String[] valElems = value.toString().split(",");
                int month = Integer.parseInt(valElems[FMONTH]);
                int deathCount = (int) Float.parseFloat(valElems[FCOUNT]);
                countByMonth[month - 1] += deathCount;
            }

            int maxMonth = 0;
            int maxCount = 0;
            for(int i = 0; i < countByMonth.length; i++) {
                if(countByMonth[i] > maxCount) {
                    maxMonth = i + 1;
                    maxCount = countByMonth[i];
                }
            }

            valRes.set(maxMonth);
            context.write(key, valRes);
        }
    }

    public static void main(String...args) throws Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config, JOB_NAME);
        job.setJarByClass(CountyMonths.class);

        job.setMapperClass(CountyMapper.class);
        job.setReducerClass(CountyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
