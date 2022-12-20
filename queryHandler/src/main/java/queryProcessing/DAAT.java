package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndex;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.Vocabulary;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;

import java.util.*;


public class DAAT {

    private static final Vocabulary vocabulary = Vocabulary.getInstance();
    private static final DocumentIndex docIndex = DocumentIndex.getInstance();
    /** method to move the iterators of postingsToScore to the given docid
     * @param docidToProcess: docid to which the iterators must be moved to
     * @return -1 if there is at least a list for which there is no docid >= docidToProcess
     */

    /**
     * parameters for BM25 scoring
     */
    private static final double k1 = 1.5;
    private static final double b = 0.75;

    /**
     * number of documents in the collection
     */
    private static final long N = CollectionSize.getCollectionSize();

    /**
     * @param postingToScore contains posting list of term we want to score
     * @return score computed according to BM25
     */
    private static double computeBM25(PostingList postingToScore){


        //get idf of term
        double idf = vocabulary.getIdf(postingToScore.getTerm());
        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(postingToScore.getPostings().get(0).getValue()));
        //get document length
        int docLen = docIndex.getLength(postingToScore.getPostings().get(0).getKey());
        //get average document length
        double avgDocLen = (double) CollectionSize.getTotalDocLen()/N;

        //return score
        return idf * tf  / ( tf + k1 * (1 - b + b * docLen/avgDocLen));

    }

    /**
     * @param postingToScore contains posting list of term we want to score
     * @return score computed according to tfidf
     */
    private static double computeTFIDF(PostingList postingToScore){

        //get idf of term
        double idf = vocabulary.getIdf(postingToScore.getTerm());
        //get frequency of term occurring in document we are scoring
        double tf = (1 + Math.log10(postingToScore.getPostings().get(0).getValue()));

        System.out.println(postingToScore);
        System.out.println("tf-> " + tf);

        //return score
        return idf * tf ;
    }

     /** method to move the iterators of postingsToScore to the given docid
     * @param docidToProcess: docid to which the iterators must be moved to
     * @return -1 if there is at least a list for which there is no docid >= docidToProcess
     */
    private static int nextGEQ(int docidToProcess, ArrayList<PostingList> postingsToScore){
        // move the iterators for posting lists pointing to docids < docidToProcess

        System.out.println("moving iterators towards docid: \t"+docidToProcess);
        System.out.println("iterators:\t"+postingsToScore);

        int nextGEQ = docidToProcess;

        for(int i=0; i<postingsToScore.size(); i++){
            // i-th posting list
            PostingList currPostingList = postingsToScore.get(i);

            // check if there are postings to iterate in the i-th posting list
            if(currPostingList != null){
                // I should move the iterator until I find a docid >= docidToProcess
                while(currPostingList.getPostings() != null && !currPostingList.getPostings().isEmpty() && currPostingList.getPostings().get(0).getKey() < nextGEQ)
                    currPostingList.getPostings().remove(0);

                // check if in the current posting list there is no docid >= docidToProcess to be processed
                if(currPostingList.getPostings() == null || currPostingList.getPostings().isEmpty()){
                    System.out.println("conjunctive mode end");
                    return -1;
                }

                // check if in the current posting list is not present docidToProcess but it is present a docid >=
                if (currPostingList.getPostings().get(0).getKey()>nextGEQ) {
                    // the current docid will be the candidate next docid to be processed

                    // set nextGEQ to new value
                    nextGEQ = currPostingList.getPostings().get(0).getKey();
                    i=-1;
                    System.out.println("nextGEQ updated to"+nextGEQ);
                    // move the iterators of other posting lists to new next docid to be processed
                    //return nextGEQ(currPostingList.getPostings().get(0).getKey(), postingsToScore);

                }
            }
        }

        System.out.println("conjunctive mode, next docid to be processed:\t"+docidToProcess);
        System.out.println("moved iterators:\t"+postingsToScore);
        return nextGEQ;
    }

    /** method to find next document to be processed among all the postings to be scored
     * @return
     * - if query mode is DISJUNCTIVE, return the minimum docid among all the first docids in the posting lists of the array, -1 if not possible
     * - else, query mode is CONJUNCTIVE, return the maximum docid among all the first docids in the posting lists of the array,
     *      it returns -1 if the maximum docid is not present in all the posting lists to be scored
     * */
    private static int nextDocToProcess(boolean isConjunctive, ArrayList<PostingList>postingsToScore){
        int docidToProcess = -1;

       // System.out.println("finding next doc to score among:\t"+postingsToScore);

        // go through all the posting lists of other query terms
        for(int i=0; i<postingsToScore.size(); i++){
            // i-th posting list
            PostingList currPostingList = postingsToScore.get(i);

            // check if there are postings to iterate in the i-th posting list
            if(currPostingList != null && currPostingList.getPostings() != null && !currPostingList.getPostings().isEmpty()){
                // retrieve docid of the first document in the pointed posting list
                int pointedDocid = currPostingList.getPostings().get(0).getKey();

                //System.out.println("curr iterator points to:\t"+pointedDocid);

                if(!isConjunctive){
                    // DISJUNCTIVE MODE
                    // search for the minimum docid

                    // update minDocid
                    if(docidToProcess == -1 || pointedDocid < docidToProcess)
                        docidToProcess = pointedDocid;
                }
                else{
                    // CONJUNCTIVE MODE
                    // search for the maximum docid

                    // update maximum docid
                    if(pointedDocid > docidToProcess)
                        docidToProcess = pointedDocid;
                }
            }
            //System.out.println("next docid value after this iteration:\t"+docidToProcess);
        }
        if(isConjunctive)
            return nextGEQ(docidToProcess, postingsToScore);
        else
            return docidToProcess;
    }

    /**
     * method to compute the IDF score of a particular document identified by docid
     * @param docid : docid of the document to be scored
     * @param scoringFunction
     * @return score of the document
     */
    private static double scoreDocument(int docid, ArrayList<PostingList> postingsToScore, String scoringFunction){
        // initialization of document's score
        double docScore = 0;

        // find postings about the docid to be processed
        for(PostingList postingList : postingsToScore) {
            // check if the current postinglist is pointing to the docid we are currently processing

            if (postingList.getPostings() != null && !postingList.getPostings().isEmpty() && postingList.getPostings().get(0).getKey() == docid) {
                // process the posting

                if(scoringFunction.equals("tfidf"))
                    docScore += computeTFIDF(postingList);
                else
                    docScore += computeBM25(postingList);

                // posting scored, it can be removed by the postings to be scored
                postingList.getPostings().remove(0);
            }
        }
        return docScore;
    }

    /** method to process DAAT a list of posting list of the query terms using TFIDF as scoring function
     * @param queryPostings : list of postings of query terms
     * @param isConjuctive : if true, the query must be processed in CONJUNCTIVE way, else in DISJUNCTIVE way
     * @param k : number of top k documents to be returned
     * @param scoringFunction
     * @return returns a priority queue (of at most K elements) in the format <SCORE (Double), DOCID (Integer)> ordered by increasing score value
     */
    public static PriorityQueue<Map.Entry<Double, Integer>> scoreQuery(ArrayList<PostingList> queryPostings, boolean isConjuctive, int k, String scoringFunction){

        // TODO: implement deep copy or iterators

        System.out.println("query postings:\t"+ queryPostings);

        ArrayList<PostingList> copyPostings = new ArrayList<>();

        for (PostingList pl : queryPostings) {
            PostingList copy = new PostingList();
            ArrayList<Map.Entry<Integer, Integer>> postings = new ArrayList<>();
            for (Map.Entry<Integer, Integer> posting : pl.getPostings()) {
                postings.add(new AbstractMap.SimpleEntry<>(posting.getKey(), posting.getValue()));
            }
            copy.appendPostings(postings);
            copy.setTerm(pl.getTerm());
            copyPostings.add(copy);
        }

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        int docToProcess = nextDocToProcess(isConjuctive, copyPostings);

        // until there are documents to be processed
        while(docToProcess!= -1){

            double docScore = scoreDocument(docToProcess, copyPostings,scoringFunction);

            // check if the MinHeap is full
            if(topKDocuments.size()==k){
                // MinHeap is full
                //System.out.println("heap is full\t");
                // check if the processed document can enter the MinHeap
                if (docScore > topKDocuments.peek().getKey()) {
                    //System.out.println("current enters the heap\t");
                    // the current score enters the MinHeap

                    // remove the root of the MinHeap (the lowest score in top K documents)
                    topKDocuments.poll();

                    // insert the document and its score in the MinHeap
                    topKDocuments.add(new AbstractMap.SimpleEntry<>(docScore, docToProcess));
                }
            } else {
                // MinHeap is not full, the current document enters the MinHeap
               // System.out.println("heap is not full\t");
                // insert the document and its score in the MinHeap
                topKDocuments.add(new AbstractMap.SimpleEntry<>(docScore, docToProcess));
            }

            // find next document to be processed
            docToProcess = nextDocToProcess(isConjuctive, copyPostings);
        }
       // System.out.println("top K:\t"+topKDocuments);
        return topKDocuments;
    }

}
