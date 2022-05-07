/**
 * Copyright (c) 2014 Daniele Pantaleone <danielepantaleone@icloud.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @author Daniele Pantaleone
 * @version 1.0
 * @copyright Daniele Pantaleone, 17 October, 2014
 * @package it.uniroma1.hadoop.pagerank
 */

 /// NORMAL
 /// javac -sourcepath src -d build src/**/*.java
 /// javac PageRank.java /**/*.java

 /// HADOOP
 /// hadoop com.sun.tools.javac.Main InvertedIndex.java
 /// hadoop com.sun.tools.javac.Main -d build/ PageRank.java **/*.java

// package it.uniroma1.hadoop.pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;

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

// MAP 
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//exception
import java.nio.charset.CharacterCodingException;


public class PageRank {

public static class XmlInputFormat extends TextInputFormat {

    public static final String START_TAG_KEY = "<page>";
    public static final String END_TAG_KEY = "</page>";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        try {
            return new XmlRecordReader((FileSplit) split, context);
        } catch (IOException e) {
            throw new RuntimeException("TODO : refactor this...");
        }
    }

    /**
     * XMLRecordReader class to read through a given xml document to output xml
     * blocks as records as specified by the start tag and end tag
     */
    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private final byte[] startTag;
        private final byte[] endTag;
        private final long start;
        private final long end;
        private static  FSDataInputStream fsin;
        private static  DataOutputBuffer buffer = new DataOutputBuffer();

        private LongWritable key = new LongWritable();
        private Text value = new Text();


        public XmlRecordReader(FileSplit split, TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
            conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
            startTag  = START_TAG_KEY.getBytes("utf-8");
            endTag  = END_TAG_KEY.getBytes("utf-8");

            //startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
            //endTag = conf.get(END_TAG_KEY).getBytes("utf-8");

            // open the file and seek to the start of the split
            start = split.getStart();
            end = start + split.getLength();
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(split.getPath());
            fsin.seek(start);
        }
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0, buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
        @Override
        public void close() throws IOException {
            fsin.close();
        }
        @Override
        public float getProgress() throws IOException {
            return (fsin.getPos() - start) / (float) (end - start);
        }
        
        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return false;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length) return true;
                } else i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
            }
        }
    }
}

public static class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
        
        String pageString = titleAndText[0];
        if(notValidPage(pageString))
            return;
        
        Text page = new Text(pageString.replace(' ', '_'));

        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
        
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String otherPage = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty()) 
                continue;
            
            // add valid otherPages to the map.
            context.write(page, new Text(otherPage));
            
            //nodeA     nodeB
            //nodeA     nodeC
        }
    }
    
    private boolean notValidPage(String pageString) {
        return pageString.contains(":");
    }

    private String getWikiPageFromLink(String aLink){
        if(isNotWikiLink(aLink)) return null;
        
        int start = aLink.startsWith("[[") ? 2 : 1;
        int endLink = aLink.indexOf("]");

        int pipePosition = aLink.indexOf("|");
        if(pipePosition > 0){
            endLink = pipePosition;
        }
        
        int part = aLink.indexOf("#");
        if(part > 0){
            endLink = part;
        }
        
        aLink =  aLink.substring(start, endLink);
        aLink = aLink.replaceAll("\\s", "_");
        aLink = aLink.replaceAll(",", "");
        aLink = sweetify(aLink);
        
        return aLink;
    }
    
    private String sweetify(String aLinkText) {
        if(aLinkText.contains("&amp;"))
            return aLinkText.replace("&amp;", "&");

        return aLinkText;
    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndText = new String[2];
        
        int start = value.find("<title>");
        int end = value.find("</title>", start);
        start += 7; //add <title> length.
        
        titleAndText[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("<text");
        start = value.find(">", start);
        end = value.find("</text>", start);
        start += 1;
        
        if(start == -1 || end == -1) {
            return new String[]{"",""};
        }
        
        titleAndText[1] = Text.decode(value.getBytes(), start, end-start);
        
        return titleAndText;
    }

    private boolean isNotWikiLink(String aLink) {
        int start = 1;
        if(aLink.startsWith("[[")){
            start = 2;
        }
        
        if( aLink.length() < start+2 || aLink.length() > 100) return true;
        char firstChar = aLink.charAt(start);
        
        if( firstChar == '#') return true;
        if( firstChar == ',') return true;
        if( firstChar == '.') return true;
        if( firstChar == '&') return true;
        if( firstChar == '\'') return true;
        if( firstChar == '-') return true;
        if( firstChar == '{') return true;
        
        if( aLink.contains(":")) return true; // Matches: external links and translations links
        if( aLink.contains(",")) return true; // Matches: external links and translations links
        if( aLink.contains("&")) return true;
        
        return false;
    }
}

public static class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Job#1 mapper will simply parse a line of the input graph creating a map with key-value(s) pairs.
         * Input format is the following (separator is TAB):
         * 
         *     <nodeA>    <nodeB>
         * 
         * which denotes an edge going from <nodeA> to <nodeB>.
         * We would need to skip comment lines (denoted by the # characters at the beginning of the line).
         * We will also collect all the distinct nodes in our graph: this is needed to compute the initial 
         * pagerank value in Job #1 reducer and also in later jobs.
         */

         /*
         Input: Titles
         Search text with API: Text
         Search links in Text
         
         Output: <nodeA> <nodeB>
         */
        
        if (value.charAt(0) != '#') {
            
            int tabIndex = value.find("\t");
            String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
            String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
            context.write(new Text(nodeA), new Text(nodeB));
            
            // add the current source node to the node list so we can 
            // compute the total amount of nodes of our graph in Job#2
            PageRank.NODES.add(nodeA);
            // also add the target node to the same list: we may have a target node 
            // with no outlinks (so it will never be parsed as source)
            PageRank.NODES.add(nodeB);
        }
    }
}

public static class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        /* Job#1 reducer will scroll all the nodes pointed by the given "key" node, constructing a
         * comma separated list of values and initializing the page rank for the "key" node.
         * Output format is the following (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,...,<linkN>
         *     
         * As for the pagerank initial value, early version of the PageRank algorithm used 1.0 as default, 
         * however later versions of PageRank assume a probability distribution between 0 and 1, hence the 
         * initial valus is set to DAMPING FACTOR / TOTAL NODES for each node in the graph.   
         */
        
        boolean first = true;
        //String links = (PageRank.DAMPING / PageRank.NODES.size()) + "\t";
        String links = "0.85" + "\t";

        for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }

        context.write(key, new Text(links));
    }

}

public  static class PageRankJob2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* PageRank calculation algorithm (mapper)
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * Output has 2 kind of records:
         * One record composed by the collection of links of each page:
         *     
         *     <title>   |<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record composed by the linked page, the page rank of the source page 
         * and the total amount of out links of the source page:
         *  
         *     <link>    <page-rank>    <total-links>
         */
        
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
        String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
        
        String[] allOtherPages = links.split(",");
        for (String otherPage : allOtherPages) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        
        // put the original links so the reducer is able to produce the correct output
        context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
        
    }
    
}

public static class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        
        /* PageRank calculation algorithm (reducer)
         * Input file format has 2 kind of records (separator is TAB):
         * 
         * One record composed by the collection of links of each page:
         * 
         *     <title>   |<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record composed by the linked page, the page rank of the source page 
         * and the total amount of out links of the source page:
         *
         *     <link>    <page-rank>    <total-links>
         */
        
        String links = "";
        double sumShareOtherPageRanks = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith(PageRank.LINKS_SEPARATOR)) {
                // if this value contains node links append them to the 'links' string
                // for future use: this is needed to reconstruct the input for Job#2 mapper
                // in case of multiple iterations of it.
                links += content.substring(PageRank.LINKS_SEPARATOR.length());
            } else {
                
                String[] split = content.split("\\t");
                
                // extract tokens
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                
                // add the contribution of all the pages having an outlink pointing 
                // to the current node: we will add the DAMPING factor later when recomputing
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPageRanks += (pageRank / totalLinks);
            }

        }
        
        double newRank = PageRank.DAMPING * sumShareOtherPageRanks + (1 - PageRank.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
        
    }

}

public static class PageRankJob3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Rank Ordering (mapper only)
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * This is a simple job which does the ordering of our documents according to the computed pagerank.
         * We will map the pagerank (key) to its value (page) and Hadoop will do the sorting on keys for us.
         * There is no need to implement a reducer: the mapping and sorting is enough for our purpose.
         */
        
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}
    
    // args keys
    private static final String KEY_DAMPING = "--damping";
    private static final String KEY_DAMPING_ALIAS = "-d";
    
    private static final String KEY_COUNT = "--count";
    private static final String KEY_COUNT_ALIAS = "-c";
    
    private static final String KEY_INPUT = "--input";
    private static final String KEY_INPUT_ALIAS = "-i";
    
    private static final String KEY_OUTPUT = "--output";
    private static final String KEY_OUTPUT_ALIAS = "-o"; 
    
    private static final String KEY_HELP = "--help";
    private static final String KEY_HELP_ALIAS = "-h"; 
    
    // utility attributes
    public static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    
    // configuration values
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 2;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    
    
    /**
     * This is the main class run against the Hadoop cluster.
     * It will run all the jobs needed for the PageRank algorithm.
     */
    public static void main(String[] args) throws Exception {
        // System.out.println(args);
        // printUsageText("info");

        try {
            
            // parse input parameters
            for (int i = 0; i < args.length; i += 2) {
               
                String key = args[i];
                String value = args[i + 1];
                // NOTE: do not use a switch to keep Java 1.6 compatibility!
                if (key.equals(KEY_DAMPING) || key.equals(KEY_DAMPING_ALIAS)) {
                    // be sure to have a damping factor in the interval [0:1]
                    PageRank.DAMPING = Math.max(Math.min(Double.parseDouble(value), 1.0), 0.0);
                } else if (key.equals(KEY_COUNT) || key.equals(KEY_COUNT_ALIAS)) {
                    // be sure to have at least 1 iteration for the PageRank algorithm
                    PageRank.ITERATIONS = Math.max(Integer.parseInt(value), 1);
                } else if (key.equals(KEY_INPUT) || key.equals(KEY_INPUT_ALIAS)) {
                    PageRank.IN_PATH = value.trim();
                    if (PageRank.IN_PATH.charAt(PageRank.IN_PATH.length() - 1) == '/')
                        PageRank.IN_PATH = PageRank.IN_PATH.substring(0, PageRank.IN_PATH.length() - 1);
                } else if (key.equals(KEY_OUTPUT) || key.equals(KEY_OUTPUT_ALIAS)) {
                    PageRank.OUT_PATH = value.trim();
                    if (PageRank.OUT_PATH.charAt(PageRank.OUT_PATH.length() - 1) == '/')
                        PageRank.OUT_PATH = PageRank.OUT_PATH.substring(0, PageRank.IN_PATH.length() - 1);
                } else if (key.equals(KEY_HELP) || key.equals(KEY_HELP_ALIAS)) {
                    printUsageText(null);
                    System.exit(0);                        
                }
            }
            
        } catch (ArrayIndexOutOfBoundsException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        } catch (NumberFormatException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        }
        
        // check for valid parameters to be set
        if (PageRank.IN_PATH.isEmpty() || PageRank.OUT_PATH.isEmpty()) {
            printUsageText("missing required parameters");
            System.exit(1);
        }
        
        // delete output path if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(PageRank.OUT_PATH)))
            fs.delete(new Path(PageRank.OUT_PATH), true);
        
        // print current configuration in the console
        System.out.println("Damping factor: " + PageRank.DAMPING);
        System.out.println("Number of iterations: " + PageRank.ITERATIONS);
        System.out.println("Input directory: " + PageRank.IN_PATH);
        System.out.println("Output directory: " + PageRank.OUT_PATH);
        System.out.println("---------------------------");
        
        Thread.sleep(1000);
        
        String inPath = null;;
        String lastOutPath = null;
        PageRank pagerank = new PageRank();
        
        System.out.println("Running Job#1 (graph parsing) ...");
        boolean isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/iter00");
        if (!isCompleted) {
            System.exit(1);
        }
        
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUT_PATH + "/iter" + NF.format(runs);
            lastOutPath = OUT_PATH + "/iter" + NF.format(runs + 1);
            System.out.println("Running Job#2 [" + (runs + 1) + "/" + PageRank.ITERATIONS + "] (PageRank calculation) ...");
            isCompleted = pagerank.job2(inPath, lastOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        
        System.out.println("Running Job#3 (rank ordering) ...");
        isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("DONE!");
        System.exit(0);
    }



    /**
     * This will run the Job #1 (Graph Parsing).
     * Will parse the graph given as input and initialize the page rank.
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job1(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        // job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setMapperClass(PageRankJob1Mapper.class);
        job.setMapperClass(WikiPageLinksMapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
        
        return job.waitForCompletion(true);
     
    }
    
    /**
     * This will run the Job #2 (Rank Calculation).
     * It calculates the new ranking and generates the same output format as the input, 
     * so this job can run multiple times (more iterations will increase accuracy).
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job2(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * This will run the Job #3 (Rank Ordering).
     * It will sort documents according to their page rank value.
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job3(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        
        // output
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * Print the main an only help text in the System.out
     * 
     * @param err an optional error message to display
     */
    public static void printUsageText(String err) {
        
        if (err != null) {
            // if error has been given, print it
            System.err.println("ERROR: " + err + ".\n");
        }
       
        System.out.println("Usage: pagerank.jar " + KEY_INPUT + " <input> " + KEY_OUTPUT + " <output>\n");
        System.out.println("Options:\n");
        System.out.println("    " + KEY_INPUT + "    (" + KEY_INPUT_ALIAS + ")    <input>       The directory of the input graph [REQUIRED]");
        System.out.println("    " + KEY_OUTPUT + "   (" + KEY_OUTPUT_ALIAS + ")    <output>      The directory of the output result [REQUIRED]");
        System.out.println("    " + KEY_DAMPING + "  (" + KEY_DAMPING_ALIAS + ")    <damping>     The damping factor [OPTIONAL]");
        System.out.println("    " + KEY_COUNT + "    (" + KEY_COUNT_ALIAS + ")    <iterations>  The amount of iterations [OPTIONAL]");
        System.out.println("    " + KEY_HELP + "     (" + KEY_HELP_ALIAS + ")                  Display the help text\n");
    }
    
}