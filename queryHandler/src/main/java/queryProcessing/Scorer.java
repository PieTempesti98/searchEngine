package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndex;
import it.unipi.dii.aide.mircv.common.beans.Posting;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;

/**
 * class used to apply the scoring function
 */
public class Scorer {

    /**
     * parameter k1 for BM25
     */
    private static final double k1 = 1.5;
    /**
     * parameter b for BM25
     */
    private static final double b = 0.75;

    /**
     * score the posting using the specified scoring function
     *
     * @param posting         the posting on which the scoring is performed
     * @param idf             the idf of the term related to the posting
     * @param scoringFunction the scoring function to use
     * @return the score for the posting
     */
    public static double scoreDocument(Posting posting, double idf, String scoringFunction) {
        return ((scoringFunction.equals("bm25")) ? computeBM25(posting, idf) : computeTFIDF(posting, idf));
    }

    /**
     * computes the BM25 scoring function
     *
     * @param posting the posting to score
     * @param idf     the idf to use
     * @return the score computed
     */
    private static double computeBM25(Posting posting, double idf) {

        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(posting.getFrequency()));
        //get document length
        int docLen = DocumentIndex.getInstance().getLength(posting.getDocid());
        //get average document length
        double avgDocLen = (double) CollectionSize.getTotalDocLen() / CollectionSize.getCollectionSize();

        //return score
        return idf * tf / (tf + k1 * (1 - b + b * docLen / avgDocLen));

    }

    /**
     * computes the TFIDF scoring function
     *
     * @param posting the posting to score
     * @param idf     the idf to use
     * @return the score computed
     */
    private static double computeTFIDF(Posting posting, double idf) {
        //return score
        return idf * (1 + Math.log10(posting.getFrequency()));
    }
}
