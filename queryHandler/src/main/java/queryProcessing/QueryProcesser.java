package queryProcessing;


import indexLoading.IndexLoader;
import it.unipi.dii.aide.mircv.common.beans.*;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;

import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.ArrayList;

public class QueryProcesser {

    private final Vocabulary vocabulary = Vocabulary.getInstance();

    private DocumentIndex documentIndex = DocumentIndex.getInstance();

    private ArrayList<PostingList> getQueryPostings(ProcessedDocument query){
        // ArrayList with all the posting lists
        ArrayList<PostingList> queryPostings = new ArrayList<>();
        for(String queryTerm: query.getTokens()){
            //
            queryPostings.add(IndexLoader.loadTerm(vocabulary.get(queryTerm)));

        }
        return queryPostings;
    }

    public String[] processQuery(String query, int k, boolean isConjunctive){

    /**
     * path to file storing inverted index
     */
    private static final String PATH_TO_INVERTED_INDEX = ConfigurationParameters.getInvertedIndexPath();


    /**
     * checks if the data structures needed for query processing were correctly created
     * @return boolean
     */
    public static boolean setupProcesser(){


        // load the posting lists of the tokens
        ArrayList<PostingList> queryPostings = getQueryPostings(processedQuery);



        //check if document index exists. If not the setup failed
        if(! new File(PATH_TO_INVERTED_INDEX).exists())
            return false;


        //check if vocabulary and document index were correctly created. If not the setup failed
        if(vocabulary.isEmpty() || documentIndex.isEmpty())
            return false;

        //successful setup
        return true;
    }


}
