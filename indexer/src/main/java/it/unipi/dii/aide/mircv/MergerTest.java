package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.algorithms.Merger;
import it.unipi.dii.aide.mircv.beans.PostingList;

import java.util.*;


public class MergerTest {
    public static void main(String[] args) {

        /*----------- TEST 1 ------------*/
        System.out.println("\n/*----------- TEST 1 ------------*/");

        List<PostingList> index1 = new ArrayList<>();

        index1.add(new PostingList("cioccolata\t1:3 2:4: 3:7"));
        index1.add(new PostingList("gelato\t1:5 3:2: 4:6"));
        index1.add(new PostingList("pizza\t2:1 5:3"));

        List<PostingList> result1 = index1;

        Map<Integer, List<PostingList>> intermediateIndexes = new HashMap<>();

        intermediateIndexes.put(0, index1);


        System.out.println("\nINTERMEDIATE INDEXES:");
        System.out.println(index1.toString());

        List<PostingList> mergedIndex1 =  Merger.testMerger(intermediateIndexes);

        System.out.println("\nEXPECTED RESULTS:");
        System.out.println(result1);

        System.out.println("\nACTUAL RESULTS:");
        System.out.println(mergedIndex1.toString());

        assert mergedIndex1 == result1 : "\nERROR IN TEST 1\n";

        System.out.println("\nTEST 1 ENDED SUCCESSFULLY\n");

        /*----------- TEST 2 ------------*/
        System.out.println("/*----------- TEST 2 ------------*/");
        List<PostingList> index2 = new ArrayList<>();

        index2.add(new PostingList("binocolo\t6:1 7:2"));
        index2.add(new PostingList("pizza\t6:2 7:9: 8:4"));
        index2.add(new PostingList("trottola\t7:5 8:2"));

        List<PostingList> result2 = new ArrayList<>();
        result2.add(new PostingList("binocolo\t6:1 7:2"));
        result2.add(new PostingList("cioccolata\t1:3 2:4: 3:7"));
        result2.add(new PostingList("gelato\t1:5 3:2: 4:6"));
        result2.add(new PostingList("pizza\t2:1 5:3 6:2 7:9 8:4"));
        result2.add(new PostingList("trottola\t7:5 8:2"));

        intermediateIndexes.put(1, index2);

        System.out.println("\nINTERMEDIATE INDEXES:");
        System.out.println(index1.toString());
        System.out.println(index2.toString());

        System.out.println("\nEXPECTED RESULTS:");
        System.out.println(result2.toString());


        List<PostingList> mergedIndex2 =  Merger.testMerger(intermediateIndexes);


        System.out.println("\nACTUAL RESULTS:");
        System.out.println(mergedIndex2.toString());


        assert mergedIndex2 == result2 : "\nERROR IN TEST 2\n";

        System.out.println("\nTEST 2 ENDED SUCCESSFULLY\n");

        /*----------- TEST 3 ------------*/

        /*
        System.out.println("----------- TEST 3 ------------");

        System.out.println("\tINTERMEDIATE INDEXES:");
        System.out.println(index1.toString());
        System.out.println(index2.toString());

        System.out.println("\tEXPECTED VOCABULARY:");


        List<VocabularyEntry> testVocabulary =  Merger.testVocabularyCreation(intermediateIndexes);


        System.out.println("\tACTUAL RESULTS:");
        System.out.println(testVocabulary.toString());


        assert(mergedIndex2 == result2);

        System.out.println("TEST 2 ENDED SUCCESSFULLY");
        */
    }
}
