import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCount extends Configured implements Tool {

    static int printUsage() {
        System.out.println("WordCount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class WordCountMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        // so we don't have to do reallocations
        private final static DoubleWritable total = new DoubleWritable(0.0);
        private Text word = new Text();
        // to chec for only alphanumeric
        //String expression1 = "^[a-zA-Z]*$";
        //String expression1 = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
        //String expression2 = "\\d{1,3}(\\.\\d{1,3})?";


        // Pattern pattern1 = Pattern.compile(expression1);
        //Pattern pattern2 = Pattern.compile(expression2);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] arr = line.split(",");

            if (arr.length == 11 && !arr[0].equals("medallion")){

                String date = arr[3].split(" ")[0];
                //Text tdate = new Text(date);
                //throw new RuntimeException ("check string date " + date + " check Text date " + tdate);

                word.set(date);
                //throw new RuntimeException ("check word " + word );
                total.set(Double.parseDouble(arr[10]));
                context.write(word, total);

            }


             /*
                //StringTokenizer itr = new StringTokenizer(value.toString(),",");
                //int count = 0;

                while (itr.hasMoreTokens()) {

                    // check for all alphabetical
                    String nextToken = itr.nextToken();

                    Matcher matcher1 = pattern1.matcher(nextToken);
                    Matcher matcher2 = pattern2.matcher(nextToken);



                    if (matcher1.matches()){

                        Text date = new Text(nextToken.split(" ")[0]);
                        word.set(date);

                    }else if(matcher2.matches()){

                        count = count + 1;
                        if (count == 6){

                            double revenue = Double.parseDouble(nextToken);
                            total.set(revenue);
                            x
                            count = 0;
                        }
                    }
                }

             */
        }
    }

    public static class WordCountReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i - 1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, other_args.get(0));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

}
