package queryProcessing;


import indexLoading.IndexLoader;
import it.unipi.dii.aide.mircv.common.beans.*;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Processor of a query: receives the query text and returns the top-k documents
 */
public class QueryProcesser {

    //TODO: add flags for query processing

    /**
     * Vocabulary (already loaded in memory)
     */
    private static final Vocabulary vocabulary = Vocabulary.getInstance();

    /**
     * Document index (already loaded in memory
     */
    private static final DocumentIndex documentIndex = DocumentIndex.getInstance();

    /**
     * path to file storing inverted index docids
     */
    private static final String INVERTED_INDEX_DOCIDS_PATH = ConfigurationParameters.getInvertedIndexDocs();

    /**
     * path to file storing inverted index frequencies
     */
    private static final String INVERTED_INDEX_FREQS_PATH = ConfigurationParameters.getInvertedIndexFreqs();

    /**
     * load from disk the posting lists of the query tokens
     * @param query the query document
     * @return the list of the query terms' posting lists
     */
    private static ArrayList<PostingList> getQueryPostings(ProcessedDocument query){

        // ArrayList with all the posting lists
        ArrayList<PostingList> queryPostings = new ArrayList<>();

        ArrayList<String> queryTerms = query.getTokens();
        //remove duplicates
        queryTerms = (ArrayList<String>) queryTerms.stream()
                .distinct()
                .collect(Collectors.toList());

        for(String queryTerm: queryTerms){
            if(vocabulary.getEntry(queryTerm) == null){
                continue;
            }
            //TODO: add vocabulary entry

            //load the posting lists
            queryPostings.add(IndexLoader.loadTerm(vocabulary.getEntry(queryTerm)));
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
     * @param scoringFunction
     * @return an array with the top-k document pids
     */
    public static String[] processQuery(String query, int k, boolean isConjunctive, String scoringFunction){
        ProcessedDocument processedQuery = Preprocesser.processDocument(new TextDocument("query", query));
        // load the posting lists of the tokens
        //TODO: modify get query postings
        ArrayList<PostingList> queryPostings = getQueryPostings(processedQuery);
        if(queryPostings.isEmpty()){
            return null;
        }
        PriorityQueue<Map.Entry<Double, Integer>> priorityQueue = DAAT.scoreQuery(queryPostings, isConjunctive, k,scoringFunction,null);

        return lookupPid(priorityQueue, k);
    }

    /**
     * checks if the data structures needed for query processing were correctly created
     * @return boolean
     */
    public static boolean setupProcesser(){

        //TODO: READ FLAGS from file
        //check if document index exists. If not the setup failed
        if(! new File(INVERTED_INDEX_DOCIDS_PATH).exists() || ! new File(INVERTED_INDEX_FREQS_PATH).exists())
            return false;

        // load the document index
        if(!documentIndex.loadFromDisk())
            return false;


        //check if document index contains entries. If not the setup failed
        return !documentIndex.isEmpty();


    }


}
