import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Query3MR {

    private static final int DATE = 1;
    private static final int REGION = 4;
    private static final int INTENSIVE_CARE = 8;
    private static final int TOTAL = 9;

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"Regions with highest percentages");
        job.setJarByClass(Query3MR.class);

        job.setMapperClass(Query3MR.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Query3MR.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setCombinerClass(Query3MR.MyReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{

        private Text date = new Text();
        private Text regionPercentage = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] records = value.toString().split(",");

            int intensiveCare = Integer.parseInt(records[INTENSIVE_CARE]);
            //float intensiveCare = Integer.parseInt(records[INTENSIVE_CARE])*1.0f;
            //float totalPatients = Integer.parseInt(records[TOTAL])*1.0f;
            int totalPatients = Integer.parseInt(records[TOTAL]);
            float percentage = (totalPatients!=0) ? ((float)intensiveCare/totalPatients) : 0;

            date.set(records[DATE].split("T")[0]);
            regionPercentage.set(records[REGION] + "," + percentage);

            context.write(date, regionPercentage);

        }
    }//MyMapper

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            String region = "";
            float percentage = 0f;

            for(Text t : values){
                String[] s = t.toString().split(",");
                String tmp = s[1];
                if(tmp.contains("%")) tmp = tmp.substring(0,tmp.indexOf("%"));
                float p = Float.parseFloat(tmp);
                if(p>percentage){
                    percentage = p;
                    region = s[0];
                }//}
            }
            result.set(region + "," + Float.toString(percentage));

            context.write(key, result);

        }

    }//MyReducer
    
}
