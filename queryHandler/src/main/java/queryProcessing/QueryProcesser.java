package queryProcessing;


import indexLoading.IndexLoader;
import it.unipi.dii.aide.mircv.common.beans.*;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

import java.io.File;
import java.util.*;

/**
 * Processor of a query: receives the query text and returns the top-k documents
 */
public class QueryProcesser {

    /**
     * Vocabulary (already loaded in memory)
     */
    private static final Vocabulary vocabulary = Vocabulary.getInstance();

    /**
     * Document index (already loaded in memory
     */
    private static final DocumentIndex documentIndex = DocumentIndex.getInstance();

    /**
     * path to file storing inverted index
     */
    private static final String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();

    /**
     * load from disk the posting lists of the query tokens
     * @param query the query document
     * @return the list of the query terms' posting lists
     */
    private static ArrayList<PostingList> getQueryPostings(ProcessedDocument query){
        //TODO: scan each query term only once

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

    /**
     * Lookups in the docuent index to retrieve pids of the top-k documents
     * @param priorityQueue The top scored documents
     * @param k number of documents to return
     * @return the ordered array of document pids
     */
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

    /**
     * Processes a query, computing the score for each document and returning the top-k documents
     * @param query The query string
     * @param k number of documents to retrieve
     * @param isConjunctive specifies if the query is conjunctive
     * @return an array with the top-k document pids
     */
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
