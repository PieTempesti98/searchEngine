package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;

import static it.unipi.dii.aide.mircv.utils.Utility.initializeFiles;

public class Main {
    public static void main(String[] args) {
        initializeFiles();
        Spimi.executeSpimi();
        Merger.mergeIndexes();
    }
}