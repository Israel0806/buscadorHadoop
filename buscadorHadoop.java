
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

// INPUT FORMAT
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import java.util.*;
import java.util.function.Supplier;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

// MULTIPLE MAPPERS
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class buscadorHadoop {
    public static List<String> WORDS = new ArrayList<String>();
    public static HashMap<String, Float> pageRanks;
    public static List<String> aqui = new ArrayList<String>();
    public static Integer count = 0;
    /*
     * Job1: Mapper
     * Input: word, document:# document:#
     * 
     * Output: document, word:#
     * 
     */

    public static class BuscadorJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        // InvertedIndex: word doc1:3 doc2:4
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String words = conf.get("WORDS");
            List<String> WORDS = new ArrayList<String>(
                    Arrays.asList(words.substring(1, words.length() - 1).split(",")));

            String _line = Text.decode(value.getBytes(), 0, value.getLength());
            String[] line = _line.split("\t");
            String word = line[0];

            String[] lines = line[1].split(" ");

            for (int i = 0; i < lines.length; ++i) {
                String _doc = lines[i];
                String[] doc = _doc.split(":"); // [[doc1,5]

                if (WORDS.contains(word)) {
                    context.write(new Text(doc[0]), new Text((doc[1])));
                }
            }
        }
    }

    public static class BuscadorJob1Reducer extends Reducer<Text, Text, Text, IntWritable> {
        /**
         * Job1: Reducer
         * Input: document, [#2, #24, #2] (doc1,[4,4,4])
         * Output: document, totalOcurrences GUARDAR EN UN HASH DEL BUSCADOR
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalOcurrence = 0;
            for (Text value : values)
                totalOcurrence += Integer.parseInt(value.toString());
            context.write(key, new IntWritable(totalOcurrence));
        }
    }
 
    /**
     * Mapper 1 (PageRank):
     * Input: PageRank  doc1
     * Output (doc1, PR:PageRank)
     */
    public static class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            int splitIndex = value.find("\t");
            String pageRank = value.toString().substring(0, splitIndex);
            String document = value.toString().substring(splitIndex + 1);

            context.write(new Text(document), new Text("PR:" + pageRank));  //Jean-Louis_Duport	PR:0.1500258594751358
            //context.write(new Text(document), new Text(pageRank));
        }
    }

    /**
     * Mapper 2 (Total ocurrences)
     * Input: doc1, #Ocurrences
     * Output (doc1, NO:#Ocurrences)
     */
    public static class OcurrencesJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            int splitIndex = value.find("\t");
            String document = value.toString().substring(0, splitIndex);
            String ocurrences = value.toString().substring(splitIndex + 1);

            context.write(new Text(document), new Text("NO:" + ocurrences));
            // context.write(new Text(document), new Text(ocurrences));
        }
    }

    /**
     * Reducer (#Ocurrences * PageRank)
     * Input (doc1, F1:PageRank) o (doc1, F2:#Ocurrences)
     * 
     * Output (doc1, PageRank * Ocurrences)
     */

     // 15  Doc1 0.15555
     // Doc2 1
     
    public static class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)   //key=doc
                throws IOException, InterruptedException {

            Float result = Float.parseFloat("1");
            int counter = 0;
            String acumular ="";
            for (Text value : values) {
                String val = value.toString(); // PR:2 o NO:5

                if (val.substring(0, 2).compareTo("PR") == 0) { // PageRank
                    result *= Float.parseFloat(val.substring(3));
                    ++counter;
                } else if (val.substring(0, 2).compareTo("NO") == 0) { // #Ocurrences
                    result *= Float.parseFloat(val.substring(3));
                    ++counter;
                }
                acumular=acumular+val.substring(0, 2)+",";
            }
	    
            if(counter >= 2)
                context.write(new Text(key), new Text(result.toString()));
            // context.write(new Text(key), new Text((acumular)));
            // context.write(new Text(key), new Text(Integer.toString(counter)));
            //context.write((key), new Text(acumular));     //Jean-Louis_Duport	PR:0.1500258594751358
              //context.write(new Text(key), new Text("texto"));
        }
    }

   

    public static String[] SEARCH;

    public static String INVERTEDINDEX = "";
    public static String PAGERANK = "";
    public static HashMap<String, Integer> ocurrencias;

    /*
     * --search: keywords
     * --i: Inverted filename
     * --p: PageRank filename
     */

    private static final String KEY_SEARCH = "--search";
    private static final String KEY_SEARCH_ALIAS = "-s";

    private static final String KEY_INVERTEDINDEX_FILE = "--invertedIndex";
    private static final String KEY_INVERTEDINDEX_FILE_ALIAS = "-i";

    private static final String KEY_PAGERANK_FILE = "--pageRank";
    private static final String KEY_PAGERANK_FILE_ALIAS = "-p";

    private static final String KEY_OUTPUT = "--output";
    private static final String KEY_OUTPUT_ALIAS = "-o";

    public static String IN_PATH = "";
    public static String OUT_PATH = "";

    public static void main(String[] args) throws Exception {
        buscadorHadoop buscador = new buscadorHadoop();
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];
            // NOTE: do not use a switch to keep Java 1.6 compatibility!
            if (key.equals(KEY_SEARCH) || key.equals(KEY_SEARCH_ALIAS))
                buscador.WORDS = Arrays.asList(value.trim().split(" "));
            else if (key.equals(KEY_INVERTEDINDEX_FILE) || key.equals(KEY_INVERTEDINDEX_FILE_ALIAS))
                buscador.INVERTEDINDEX = value.trim();
            else if (key.equals(KEY_PAGERANK_FILE) || key.equals(KEY_PAGERANK_FILE_ALIAS))
                buscador.PAGERANK = value.trim();
            else if (key.equals(KEY_OUTPUT) || key.equals(KEY_OUTPUT_ALIAS)) {
                buscador.OUT_PATH = value.trim();
                if (buscador.OUT_PATH.charAt(buscador.OUT_PATH.length() - 1) == '/')
                    buscador.OUT_PATH = buscador.OUT_PATH.substring(0, buscador.IN_PATH.length() - 1);
            }
        }
        System.out.println("WORDS: " + buscador.WORDS);
        System.out.println("INVERTEDINDEX: " + buscador.INVERTEDINDEX);
        System.out.println("PAGERANK: " + buscador.PAGERANK);
        System.out.println("Output directory: " + buscador.OUT_PATH);
        System.out.println("---------------------------");

        String inPath = null;
        String lastOutPath = null;

        System.out.println("Running Job#1 (graph parsing) ...");
        boolean isCompleted = buscador.job1(INVERTEDINDEX, OUT_PATH + "/iter00");
        if (!isCompleted) {
            System.exit(1);
        }
        // System.out.println("FOUND: " + buscador.aqui);
        // System.out.println("OCURRENCIAS: " + buscador.ocurrencias.size());

        System.out.println("Running Job#2 (rank ordering) ...");
        isCompleted = buscador.job2(PAGERANK, OUT_PATH + "/iter00", OUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }

        System.out.println("DONE!");
        System.exit(0);
    }

    public boolean job1(String in, String out) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Job #1");
        Configuration conf = job.getConfiguration();
        conf.set("WORDS", buscadorHadoop.WORDS.toString());

        job.setJarByClass(buscadorHadoop.class);
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        // job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setMapperClass(PageRankJob1Mapper.class);
        job.setMapperClass(BuscadorJob1Mapper.class);

        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(BuscadorJob1Reducer.class);

        return job.waitForCompletion(true);

    }

    /**
     * Mapper 1 (PageRank):
     * Input: PageRank, doc1
     * 
     * Output (doc1, PR:PageRank)
     * 
     * Mapper 2 (Total ocurrences)
     * Input: doc1, #Ocurrences
     * Output (doc1, NO:#Ocurrences)
     * 
     * Reducer (#Ocurrences * PageRank)
     * 
     * Input (doc1, PR:PageRank) o (doc1, NO:#Ocurrences) = 
            (doc1, [PR::pagerank, NO: #Ocurrences])
     * 
     */

    // pageRank, Doc: Total ocurrence, Out
    public boolean job2(String inPageRank, String inOcurrences, String out) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        Job job = Job.getInstance(new Configuration(), "Job #2");

        // input / mapper
        // FileInputFormat.setInputPaths(job, new Path(in));

        MultipleInputs.addInputPath(job, new Path(inOcurrences), TextInputFormat.class, OcurrencesJob2Mapper.class);
        MultipleInputs.addInputPath(job, new Path(inPageRank), TextInputFormat.class, PageRankJob2Mapper.class);

        job.setJarByClass(buscadorHadoop.class);
        job.setReducerClass(PageRankJob2Reducer.class);
		
	// Map / Reducer types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(TextInputFormat.class);
        // job.setMapperClass(PageRankJob2Mapper.class);

        // output
        return job.waitForCompletion(true);

    }
}