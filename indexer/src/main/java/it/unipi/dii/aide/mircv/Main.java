package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.algorithms.Spimi;

public class Main {
    public static void main(String[] args) {
        Spimi.spimi();
        Merger.mergeIndexes();
    }
}