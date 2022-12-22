package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.config.Flags;


import java.io.*;

import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class Main {

    /**
     * path to file storing flags
     */
    private static final String FLAGS_FILE_PATH = ConfigurationParameters.getFlagsFilePath();

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

        //check input and initialize flags
        if(args.length > 1) {

            for(String flag: args){

                if(flag.equals("-c")){
                    if(debugModeEnable){
                        System.out.println("Cannot enable both debug mode and integer compression at the same time");
                        return;
                    }
                    compressedWritingEnable = true;
                    continue;
                }

                if(flag.equals("-cr")){
                    compressedReadingEnable = true;
                    continue;
                }

                if(flag.equals("-s")){
                    stemStopRemovalEnable = true;
                    continue;
                }

                if(flag.equals("-d")){

                    if(compressedWritingEnable){
                        System.out.println("Cannot enable both debug mode and integer compression at the same time");
                        return;
                    }
                    debugModeEnable = true;
                    continue;
                }

                System.out.println("Flag " + flag + " not recognised!");
                return;
            }

        }

        //save to file flags that will be useful for query handling
        if(!Flags.saveFlags(compressedWritingEnable,stemStopRemovalEnable)){
            System.out.println("Error in saving configuration modes");
            return;
        }

        //initialize files and directories needed for Spimi execution
        initializeFiles();

        int numIndexes = Spimi.executeSpimi(compressedReadingEnable,stemStopRemovalEnable,debugModeEnable);
        if(numIndexes <= 0){
            System.out.println("An error occurred: no partial indexes.");
            return;
        }
        System.out.println("Spimi done!");


        if(Merger.mergeIndexes(numIndexes,compressedWritingEnable,debugModeEnable)) {
            System.out.println("Inverted index correctly created.");
            return;
        }
        System.out.println("An error occurred during merging.");
    }

}