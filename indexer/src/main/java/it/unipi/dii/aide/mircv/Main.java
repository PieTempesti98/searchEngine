package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;
import it.unipi.dii.aide.mircv.dataLoader.CollectionLoader;

import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class Main {
    public static void main(String[] args) {
        initializeFiles();
        CollectionLoader.loadCollection();
        int numIndexes = Spimi.executeSpimi();
        if(numIndexes == 0){
            System.out.println("An error occurred: no partial indexes.");
            return;
        }
        if(Merger.mergeIndexes(numIndexes)) {
            System.out.println("Inverted index correctly created.");
            return;
        }
        System.out.println("An error occurred during merging.");
    }

}