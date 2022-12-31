package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.config.Flags;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static it.unipi.dii.aide.mircv.utils.Utility.cleanUpFiles;
import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class Main {

    /**
     * @param args args[0] -> compression flag
     * */
    public static void main(String[] args){


        //if set to true, reading from compressed file is enabled
        boolean compressedReadingEnable = false;
        //if set to true, compression of inverted index is enabled
        boolean compressedWritingEnable = false;
        //if set to true, stopwords removal and stemming is enabled
        boolean stemStopRemovalEnable = false;
        //if set to true, debug mode is enabled
        boolean debugModeEnable = false;
        //if set to true, maxScore is used, If not, DAAT is used
        boolean maxScoreEnabled = false;

        //check input and initialize flags
        for (String flag : args) {

            if (flag.equals("-c")) {
                compressedWritingEnable = true;
                continue;
            }

            if (flag.equals("-cr")) {
                compressedReadingEnable = true;
                continue;
            }

            if (flag.equals("-s")) {
                stemStopRemovalEnable = true;
                continue;
            }

            if (flag.equals("-d")) {
                debugModeEnable = true;
                continue;
            }
            if (flag.equals("-maxscore")) {
                maxScoreEnabled = true;
                continue;
            }

            System.out.println("Flag " + flag + " not recognised!");
            return;
        }

        //save to file flags that will be useful for query handling
        if(!Flags.saveFlags(compressedWritingEnable,stemStopRemovalEnable,maxScoreEnabled)){
            System.out.println("Error in saving configuration modes");
            return;
        }

        //initialize files and directories needed for Spimi execution
        initializeFiles();

        System.out.println("Indexing started with parameters: " + Arrays.toString(args));
        long start = System.currentTimeMillis();
        int numIndexes = Spimi.executeSpimi(compressedReadingEnable, debugModeEnable);
        if(numIndexes <= 0){
            System.out.println("An error occurred: no partial indexes.");
            return;
        }
        long spimiTime = System.currentTimeMillis();
        formatTime(start, spimiTime, "Spimi");

        if(Merger.mergeIndexes(numIndexes,compressedWritingEnable,debugModeEnable)) {
            cleanUpFiles();

            // print to file the indexer statistics
            long stop = System.currentTimeMillis();
            formatTime(spimiTime, stop, "Merging");
            formatTime(start, stop, "Creation of inverted index");
            FileUtils.createIfNotExists("data/indexerStatistics.tsv");
            try(BufferedWriter writer = new BufferedWriter(new FileWriter("data/indexerStatistics.tsv", true));) {
                long docidSize = Files.size(Paths.get(ConfigurationParameters.getInvertedIndexDocs()));
                long freqSize = Files.size(Paths.get(ConfigurationParameters.getInvertedIndexFreqs()));
                long vocabularySize = Files.size(Paths.get(ConfigurationParameters.getVocabularyPath()));
                long docIndexSize = Files.size(Paths.get(ConfigurationParameters.getDocumentIndexPath()));
                long fullTime = stop - start;
                String stats = Arrays.toString(args) + '\t' + fullTime + '\t' + docidSize + '\t' + freqSize + '\t' + vocabularySize + '\t' + docIndexSize + '\n';
                writer.write(stats);
            }catch(Exception e){
                e.printStackTrace();
            }

            return;
        }
        System.out.println("An error occurred during merging.");
        cleanUpFiles();
    }

    private static void formatTime(long start, long end, String operation) {
        int minutes = (int) ((end - start) / (1000 * 60));
        int seconds = (int) ((end - start) / 1000) % 60;
        if(seconds < 10)
            System.out.println(operation + " done in " + minutes + ":0" + seconds + " minutes");
        else
            System.out.println(operation + " done in " + minutes + ":" + seconds + " minutes");
    }

}