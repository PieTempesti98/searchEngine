package queryProcessing.tests;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.config.Flags;
import queryProcessing.MaxScore;
import queryProcessing.QueryProcesser;

import java.util.*;

import static queryProcessing.QueryProcesser.getQueryPostings;

public class MaxScoreTest {

    private static void printResults(PriorityQueue<Map.Entry<Double, Integer>> results, int k){
        String[] res = QueryProcesser.lookupPid(results, k);
        System.out.println("MaxScore results:");
        int rank = 1;
        for(String docRes: res){
            System.out.println("\t"+(rank++) +") "+docRes);
        }
    }

    private static boolean checkResults(PriorityQueue<Map.Entry<Double, Integer>> actualResults, PriorityQueue<Map.Entry<Double, Integer>> expectedResults, boolean verbose){

        Iterator<Map.Entry<Double, Integer>> actualRes = actualResults.iterator();
        Iterator<Map.Entry<Double, Integer>> expectedRes = expectedResults.iterator();

        while(expectedRes.hasNext()){
            if(actualRes.hasNext()){
                Map.Entry<Double, Integer> expectedEntry = expectedRes.next();
                Map.Entry<Double, Integer> actualEntry = actualRes.next();
                if(!Objects.equals(expectedEntry.getKey(), actualEntry.getKey()) || !Objects.equals(expectedEntry.getValue(), actualEntry.getValue())){
                    if(verbose){
                        System.out.println("\texpected: ("+expectedEntry.getValue()+","+expectedEntry.getKey()+")");
                        System.out.println("\tfounded: ("+actualEntry.getValue()+","+actualEntry.getKey()+")");
                        System.out.println("actual results: "+actualResults);
                        System.out.println("expected results: "+expectedResults);
                    }
                   return false;
                }
            } else {
                System.out.println("\tactual results are not of same size as expected results");
                System.out.println("actual results: "+actualResults);
                System.out.println("expected results: "+expectedResults);
                return false;
            }
        }
        return true;
    }
    private static boolean test1(boolean verbose, String scoringFunction){

        System.out.println("""
                \tThe aim of this test is to test the query containing just the token "example" over the collection of documents saved in "small_collection.tsv".
                \tUsing\040""" +scoringFunction+" as scoring function, MaxScore as scoring algorithm, disjunctive mode, and returning the top 3 documents.");

        String[] query = {"example"};

        int k = 3;

        PriorityQueue<Map.Entry<Double, Integer>> expectedResults = new PriorityQueue<>(k, Map.Entry.comparingByKey());
        if(scoringFunction.equals("tfidf")){
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 4));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 2));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 5));
        } else {
            // TODO: update
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.1123005090598549, 2));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.09661547190697509, 1));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.09030875025937561, 4));
        }


        // load the posting lists of the tokens
        ArrayList<PostingList> queryPostings = getQueryPostings(new ProcessedDocument("query", query));

        if(queryPostings.isEmpty()){
            if(verbose)
                System.out.println("Error while retrieving query postings");
            return false;
        }

        PriorityQueue<Map.Entry<Double, Integer>> results = MaxScore.scoreQuery(queryPostings, k, scoringFunction);

        if(verbose)
              printResults(results, k);

        return checkResults(results, expectedResults, verbose);
    }

    private static boolean test2(boolean verbose, String scoringFunction){

        System.out.println("""
                \tThe aim of this test is to test the query containing just the tokens {"example", "another"} over the collection of documents saved in "small_collection.tsv".
                \tUsing\040""" + scoringFunction +" as scoring function, MaxScore as scoring algorithm, disjunctive mode, and returning the top 3 documents.");

        String[] query = {"example", "another"};

        int k = 3;

        PriorityQueue<Map.Entry<Double, Integer>> expectedResults = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        if(scoringFunction.equals("tfidf")) {
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.8061799739838872, 1));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 5));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.9874180905628003, 7));
        }
        else {
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.1123005090598549, 2));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.38158664142011345, 1));
            expectedResults.add(new AbstractMap.SimpleEntry<>(0.2582940702253402, 7));
        }

        // load the posting lists of the tokens
        ArrayList<PostingList> queryPostings = getQueryPostings(new ProcessedDocument("query", query));

        if(queryPostings.isEmpty()){
            if(verbose)
                System.out.println("Error while retrieving query postings");
            return false;
        }

        PriorityQueue<Map.Entry<Double, Integer>> results = MaxScore.scoreQuery(queryPostings, k, scoringFunction);

        if(verbose)
            printResults(results, k);

        return checkResults(results, expectedResults, verbose);
    }


    /**
     * this class tests MaxScore, in order to correctly test it, ensure to execute SPIMI and MERGING
     * with COMPRESSION NOT ENABLED and STOPWORDS REMOVAL NOT ENABLED
     */
    public static void main(String[] args){
        Flags.saveFlags(false, false);
        boolean setupSuccess = QueryProcesser.setupProcesser();

        if(!setupSuccess){
            System.out.println("Error in setup of this service. Shutting down...");
            return;
        }

        System.out.println("\n----------- TEST 1 ------------\n");

        if(!test1(false, "tfidf"))
            System.out.println("\nERROR: TEST 1 FAILED\n");
        else
            System.out.println("\nTEST 1 ENDED SUCCESSFULLY\n");

        System.out.println("\n----------- TEST 2 ------------\n");

        if(!test1(true, "bm25"))
            System.out.println("\nERROR: TEST 2 FAILED\n");
        else
            System.out.println("\nTEST 2 ENDED SUCCESSFULLY\n");


        System.out.println("\n----------- TEST 3 ------------\n");

        if(!test2(false, "tfidf"))
            System.out.println("\nERROR: TEST 3 FAILED\n");
        else
            System.out.println("\nTEST 3 ENDED SUCCESSFULLY\n");

        System.out.println("\n----------- TEST 4 ------------\n");

        if(!test2(false, "bm25"))
            System.out.println("\nERROR: TEST 4 FAILED\n");
        else
            System.out.println("\nTEST 4 ENDED SUCCESSFULLY\n");





    }
}
