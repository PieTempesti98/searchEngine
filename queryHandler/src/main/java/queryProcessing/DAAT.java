package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.*;

import java.util.*;


public class DAAT {

    private static void initialize(ArrayList<PostingList> queryPostings){

        for(PostingList postingList: queryPostings) {
            postingList.openList();
            postingList.next();
        }

    }

    private static void cleanUp(ArrayList<PostingList> queryPostings) {

        for(PostingList postingList: queryPostings)
            postingList.closeList();

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
                Posting pointedPosting = currPostingList.getCurrentPosting();

                if(pointedPosting == null){
                    System.out.println("conjunctive mode end");
                    return -1;
                }

                if(pointedPosting.getDocid() < nextGEQ) {
                    pointedPosting = currPostingList.nextGEQ(nextGEQ);
                    // check if in the current posting list there is no docid >= docidToProcess to be processed
                    if(pointedPosting == null){
                        System.out.println("conjunctive mode end");
                        return -1;
                    }
                }

                // check if in the current posting list is not present docidToProcess but it is present a docid >=
                if (pointedPosting.getDocid()>nextGEQ) {
                    // the current docid will be the candidate next docid to be processed

                    // set nextGEQ to new value
                    nextGEQ = pointedPosting.getDocid();
                    i=-1;
                    System.out.println("nextGEQ updated to"+nextGEQ);

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
            if(currPostingList != null &&  currPostingList.getCurrentPosting() != null){
                // retrieve docid of the first document in the pointed posting list
                int pointedDocid = currPostingList.getCurrentPosting().getDocid();

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

            Posting postingToScore = postingList.getCurrentPosting();

            if (postingToScore != null && postingToScore.getDocid() == docid) {
                // process the posting

                docScore += Scorer.scoreDocument(postingToScore, Vocabulary.getInstance().getIdf(postingList.getTerm()), scoringFunction);

                // posting scored, it can be removed by the postings to be scored
                postingList.next();
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

        initialize(queryPostings);

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        int docToProcess = nextDocToProcess(isConjuctive, queryPostings);

        // until there are documents to be processed
        while(docToProcess!= -1){

            double docScore = scoreDocument(docToProcess, queryPostings,scoringFunction);

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
            docToProcess = nextDocToProcess(isConjuctive, queryPostings);
        }
       // System.out.println("top K:\t"+topKDocuments);
        cleanUp(queryPostings);
        return topKDocuments;
    }

}
