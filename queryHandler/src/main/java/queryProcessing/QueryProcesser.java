package queryProcessing;

import indexLoading.IndexLoader;
import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

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

        // Query preprocessing
        ProcessedDocument processedQuery = Preprocesser.processDocument(new TextDocument("query", query));

        // load the posting lists of the tokens
        ArrayList<PostingList> queryPostings = getQueryPostings(processedQuery);



        //TODO: perform DAAT to compute scores and sort the documents

        // return the top-k documents
        return new String[k];
    }
}
