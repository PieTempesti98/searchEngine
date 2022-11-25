package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

public class QueryProcesser {

    public String[] processQuery(String query, int k, boolean isConjunctive){

        // Query preprocessing
        ProcessedDocument processedQuery = Preprocesser.processDocument(new TextDocument("query", query));

        //TODO: load the posting lists of the tokens

        //TODO: perform DAAT to compute scores and sort the documents

        // return the top-k documents
        return new String[k];
    }
}
