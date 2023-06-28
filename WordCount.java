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
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.regex.Pattern;
import java.util.regex.Matcher;



public class WordCount extends Configured implements Tool {

    static int printUsage() {
        System.out.println("WordCount [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }


    /********************************* Job 1 ********************************/


    public static class T2Mapper1 extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static DoubleWritable total = new DoubleWritable(0);
        private Text word = new Text();

        // to chec for only alphanumeric
        //String expression = "^[a-zA-Z]*$";
        //Pattern pattern = Pattern.compile(expression);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] arr = line.split(",");

            if (arr.length == 11 && !arr[0].equals("medallion")){

              // otherwise, write
                total.set(Double.parseDouble(arr[10]));
                word.set(arr[1]);
                //throw new RuntimeException ("check word " + word );
                context.write(word, total);
            }


            /*
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {

                    // check for all alphabetical
                    String nextToken = itr.nextToken();
                    //Matcher matcher = pattern.matcher(nextToken);

                    // if not, don't write
                    //if (!matcher.matches())
                    //continue;


                    String[] arr = nextToken.split(",");

                    if (arr[0] == "medallion"){
                        continue;
                    }


                }
            } catch (Exception e) {
            }

            */
        }
    }


    public static class T2Reducer1
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
            //throw new RuntimeException ("check key " + key );
            context.write(key, result);

            //throw new RuntimeException ("check result for all files key "+key +" result" + result );
        }
    }



    /************************ Job 2 ******************************/

    public static class Pair {
        String driver;
        double revenue;
        public Pair(String driver, double revenue) {
            this.driver = driver;
            this.revenue = revenue;
        }

        public String toString() {

            return driver + " " + revenue;
        }
    }

    public static class T2Mapper2 extends Mapper<Object, Text, IntWritable, Text> {


        private IntWritable key = new IntWritable(1);
        private Text res = new Text();
        private PriorityQueue<Pair> pqueue = new PriorityQueue<Pair>(
                new Comparator<Pair>() {
                    public int compare(Pair a, Pair b) {
                        return (int) (b.revenue - a.revenue);
                    }
                }
        );

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //throw new RuntimeException (" pause job 2" );

                String[] line = value.toString().split("\t");
                String driverid = line[0];
                double revenue = Double.parseDouble(line[1]);
                Pair p = new Pair(driverid, revenue);
                pqueue.offer(p);

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < 5; i++) {
                if (pqueue.isEmpty()) {
                    return;
                }
                Pair top5 = pqueue.poll();
                res.set(top5.toString());
                context.write(key, res);
            }
        }
    }


    public static class T2Reducer2 extends Reducer<IntWritable, Text, Text, DoubleWritable> {

        private Text id = new Text();
        private DoubleWritable result = new DoubleWritable();
        private PriorityQueue<Pair> pqueue = new PriorityQueue< >(
                new Comparator<Pair>() {
                    public int compare(Pair a, Pair b) {
                        return (int) (b.revenue - a.revenue);
                    }
                }
        );

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String[] line = val.toString().split("\\s+");
                String driver = line[0];
                double revenue = Double.parseDouble(line[1]);
                Pair p = new Pair(driver, revenue);
                pqueue.offer(p);
            }

            for (int i = 0; i < 5; i++) {
                if (pqueue.isEmpty()) {
                    return;
                }
                Pair top5 = pqueue.poll();
                id.set(top5.driver);
                result.set(top5.revenue);
                context.write(id, result);
            }
        }

    }


    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job1 = Job.getInstance(conf, "Task2Job1");
        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(T2Mapper1.class);
        //job1.setCombinerClass(T2Reducer1.class);
        job1.setReducerClass(T2Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);



        Job job2 = Job.getInstance(conf, "Task2Job2");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(T2Mapper2.class);
        //job2.setCombinerClass(T2Reducer2.class);
        job2.setReducerClass(T2Reducer2.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);



        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job1.setNumReduceTasks(Integer.parseInt(args[++i]));
                    job2.setNumReduceTasks(Integer.parseInt(args[i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }


        FileInputFormat.setInputPaths(job1, other_args.get(0));
        FileOutputFormat.setOutputPath(job1, new Path(other_args.get(1) + "/job11"));

        job1.waitForCompletion(true);
        FileInputFormat.setInputPaths(job2, other_args.get(1) + "/job11");
        FileOutputFormat.setOutputPath(job2, new Path(other_args.get(1) + "/job22"));


        return (job2.waitForCompletion(true) ? 0 : 1);

        /*
        if (job1.waitForCompletion(true)) {
            return (job2.waitForCompletion(true) ? 0 : 1);
        } else {
            return 1;
        }
        */
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

}



