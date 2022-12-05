package queryProcessing;


import indexLoading.IndexLoader;
import it.unipi.dii.aide.mircv.common.beans.*;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

import java.io.File;
import java.util.*;

public class QueryProcesser {

    private static final Vocabulary vocabulary = Vocabulary.getInstance();

    private static final DocumentIndex documentIndex = DocumentIndex.getInstance();

    /**
     * path to file storing inverted index
     */
    private static final String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();

    private static ArrayList<PostingList> getQueryPostings(ProcessedDocument query){
        // ArrayList with all the posting lists
        ArrayList<PostingList> queryPostings = new ArrayList<>();
        for(String queryTerm: query.getTokens()){
            if(vocabulary.get(queryTerm) == null){
                continue;
            }
            //load the posting list and
            queryPostings.add(IndexLoader.loadTerm(vocabulary.get(queryTerm)));
        }
        return queryPostings;
    }

    private static String[] lookupPid(PriorityQueue<Map.Entry<Double, Integer>> priorityQueue, int k){
        String[] output = new String[k];
        int i = priorityQueue.size() - 1;
        while(!priorityQueue.isEmpty()){
            int docid = priorityQueue.poll().getValue();
            output[i] = documentIndex.getPid(docid);
            i--;
        }
        return output;
    }

    public static String[] processQuery(String query, int k, boolean isConjunctive){
        ProcessedDocument processedQuery = Preprocesser.processDocument(new TextDocument("query", query));
        // load the posting lists of the tokens
        ArrayList<PostingList> queryPostings = getQueryPostings(processedQuery);
        if(queryPostings.isEmpty()){
            return null;
        }
        PriorityQueue<Map.Entry<Double, Integer>> priorityQueue = DAAT.scoreQuery(queryPostings, isConjunctive, k);

        return lookupPid(priorityQueue, k);
    }

    /**
     * checks if the data structures needed for query processing were correctly created
     * @return boolean
     */
    public static boolean setupProcesser(){

        //check if document index exists. If not the setup failed
        if(! new File(PATH_TO_INVERTED_INDEX).exists())
            return false;


        //check if vocabulary and document index were correctly created. If not the setup failed
       // if(vocabulary.isEmpty() || documentIndex.isEmpty())
           // return false;

        //successful setup
        return true;
    }


}
