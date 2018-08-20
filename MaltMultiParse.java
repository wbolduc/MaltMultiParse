/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package maltparsetest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.maltparser.concurrent.ConcurrentMaltParserModel;
import org.maltparser.concurrent.ConcurrentMaltParserService;
import org.maltparser.concurrent.ConcurrentUtils;
import org.maltparser.core.exception.MaltChainedException;

public class MaltMultiParse implements Runnable {

    private final List<String[]> inputSentences;
    private final List<String[]> outputSentences;
    private final ConcurrentMaltParserModel model;
    private static int numberOfThreads = 16;
    
    private static AtomicInteger parseCount;
    private final int progressInterval = 1000;
    
    
    public MaltMultiParse(List<String[]> sentences, ConcurrentMaltParserModel _model, AtomicInteger parseCount) {
        this.inputSentences = new ArrayList<>(sentences);
        this.outputSentences = Collections.synchronizedList(new ArrayList<>());
        this.model = _model;
        this.parseCount = parseCount;
    }

    public void run() {
        int interval = 0;
        for (int i = 0; i < inputSentences.size(); i++) {
            try {
                outputSentences.add(model.parseTokens(inputSentences.get(i)));
            } catch (MaltChainedException e) {
                e.printStackTrace();
            }

            interval++;
            if(interval == progressInterval)
            {
                parseCount.addAndGet(progressInterval);
                interval = 0;
            }           
        }
    }

    public List<String[]> getOutputSentences() {
        return Collections.synchronizedList(new ArrayList<>(outputSentences));
    }

    public static String getMessageWithElapsed(String message, long startTime) {
        final StringBuilder sb = new StringBuilder();
        long elapsed = (System.nanoTime() - startTime) / 1000000;
        sb.append(message);
        sb.append(" : ");
        sb.append(elapsed);
        sb.append(" ms");
        return sb.toString();
    }

    public static void main(String[] args){
        if (args.length == 0)
        {
            System.out.println("java -jar MaltMultiParse.jar <input Conll> <output Conll folder> <mco model>");
            return;
        }
        else if (args.length < 3)
        {
            System.out.println("Missing some arguments");
            return;
        }
        else if (args.length > 3)
        {
            System.out.println("Too many arguments");
            return;
        }
        
        File inputConll = new File(args[0]);
        if(!inputConll.isFile() && !inputConll.getName().endsWith(".conll"))
        {
            System.out.println(args[0] + " is not a valid file type");
            return;
        }
        
        File outConll = new File(args[1]);
        if(!outConll.isDirectory())
        {
            System.out.println(args[1] + " is not a directory");
            return;
        }
                
        File mcoFile = new File(args[2]);
        
        //create output file
        String inputConllName = inputConll.getName();
        outConll = new File(outConll + "\\" + inputConllName.substring(0, inputConllName.length()-6) + "-DepParsed.conll");
        
        long startTime = System.nanoTime(); 
        // Loading the model
        ConcurrentMaltParserModel maltModel = null;
        try {
            URL maltModelURL = mcoFile.toURI().toURL();
            maltModel = ConcurrentMaltParserService.initializeParserModel(maltModelURL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(getMessageWithElapsed("Loading time", startTime));
        
        //load the conll file
        startTime = System.nanoTime();
        List<String[]> inSentences = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputConll), "UTF-8"));
            while (true) {
                // Reads a sentence from the input file
                String[] conllSentence = ConcurrentUtils.readSentence(reader);
                if (conllSentence.length == 0) {
                    break;
                }
                // Strips the head and dependency edge label and add the sentence to the list of sentences
                inSentences.add(conllSentence);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(getMessageWithElapsed("Read sentences time", startTime));
        startTime = System.nanoTime();

        // Creates n threads
        if (args.length == 1) {
            int n = 0;
            try {
                n = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.out.println("Argument is not an integer.");
            }
            if (n >= 1 && n <= inSentences.size()) {
                numberOfThreads = n;
            } else {
                System.out.println("The first argument must be between 1 - " + inSentences.size());
            }
        }
        System.out.println(numberOfThreads + " threads are used to parse " + inSentences.size() + " sentences.");

        //this is just to show progress
        parseCount = new AtomicInteger();
        Thread cCounter = new Thread(new ConcurrentCounter("Sentences parsed : ", parseCount, 500));
        
        Thread[] threads = new Thread[numberOfThreads];
        MaltMultiParse[] runnables = new MaltMultiParse[numberOfThreads];
        int interval = (inSentences.size() / numberOfThreads);
        int startIndex = 0;
        int t = 0;
        while (startIndex < inSentences.size()) {
            int endIndex = (startIndex + interval < inSentences.size() && t < threads.length - 1 ? startIndex + interval : inSentences.size());
            System.out.println("  Thread " + String.format("%03d", t) + " will parse sentences between " + String.format("%04d", startIndex) + " - " + String.format("%04d", (endIndex - 1))
                    + ", number of sentences: " + (endIndex - startIndex));
            runnables[t] = new MaltMultiParse(inSentences.subList(startIndex, endIndex), maltModel, parseCount);
            threads[t] = new Thread(runnables[t]);
            startIndex = endIndex;
            t++;
        }
        System.out.println(getMessageWithElapsed("Create threads time", startTime));
        startTime = System.nanoTime();

        // Starting threads to parse all sentences.
        for (int i = 0; i < threads.length; i++) {
            if (threads[i] != null) {
                threads[i].start();
            } else {
                System.err.println("Thread " + i + " is null");
            }
        }
        //starting visual progress indicator
        cCounter.start();
        
        // Finally joining all threads
        for (int i = 0; i < threads.length; i++) {
            try {
                if (threads[i] != null) {
                    threads[i].join();
                } else {
                    System.err.println("Thread " + i + " is null");
                }
            } catch (InterruptedException ignore) {
            }
        }
        cCounter.interrupt();
        System.out.println(getMessageWithElapsed("Parsing time", startTime));
        startTime = System.nanoTime();

        // Collect parsed sentences
        List<String[]> outSentences = new ArrayList<String[]>();
        for (int i = 0; i < threads.length; i++) {
            outSentences.addAll(runnables[i].getOutputSentences());
        }
        
        //print to file
        try {
            PrintWriter writer = new PrintWriter(outConll);
            Iterator sentIter = outSentences.iterator();
            while(sentIter.hasNext())
            {
                String[] words = (String[])sentIter.next();
                for (String word : words)
                {
                    writer.println(word);
                    //System.out.println(word);
                }
                writer.println();
            }
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(MaltMultiParse.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
