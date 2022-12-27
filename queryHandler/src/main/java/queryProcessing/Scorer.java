package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndex;
import it.unipi.dii.aide.mircv.common.beans.Posting;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;

public class Scorer {
    private static final double k1 = 1.5;
    private static final double b = 0.75;

    public static double scoreDocument(Posting posting, double idf, String scoringFunction) {
        return ((scoringFunction.equals("bm25"))? computeBM25(posting, idf) : computeTFIDF(posting, idf));
    }

    private static double computeBM25(Posting posting, double idf){

        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(posting.getFrequency()));
        //get document length
        int docLen = DocumentIndex.getInstance().getLength(posting.getDocid());
        //get average document length
        double avgDocLen = (double) CollectionSize.getTotalDocLen()/CollectionSize.getCollectionSize();

        //return score
        return idf * tf / ( tf + k1 * (1 - b + b * docLen/avgDocLen));

    }

    private static double computeTFIDF(Posting posting, double idf){
        //return score
        return idf * (1 + Math.log10(posting.getFrequency())) ;
    }
}
