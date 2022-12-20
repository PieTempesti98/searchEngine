package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.common.beans.Vocabulary;


import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class Main {

    /**
     * @param args args[0] -> compression flag
     * */
    public static void main(String[] args){


        //boolean denoting whether compression will be performed or not.
        // Compression is not done by default
        boolean compress = false;

        //check input
        if(args.length > 0) {

            //check input validity
            if (args[0].equals("-c"))
                compress = true;
            else{
                System.out.println("Command not recognized. Insert -c for compression");
                return;
            }

        }


        initializeFiles();

        int numIndexes = Spimi.executeSpimi(compress);
        if(numIndexes <= 0){
            System.out.println("An error occurred: no partial indexes.");
            return;
        }
        System.out.println("Spimi done!");


        if(Merger.mergeIndexes(numIndexes,compress)) {
            System.out.println("Inverted index correctly created.");

            // TODO: remove, testing
            Vocabulary v = new Vocabulary();
            v.readFromDisk();
            System.out.println(v);

            return;
        }
        System.out.println("An error occurred during merging.");
    }

}