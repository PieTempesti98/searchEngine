package queryProcessing;

import it.unipi.dii.aide.mircv.beans.PostingList;

import java.util.*;


public class DAAT {

    /**
     * iterators to the next posting to be scored for each posting list relative to query terms
     */
    private static ArrayList<PostingList> postingsToScore;

    /** method to move the iterators of postingsToScore to the given docid
     * @param docidToProcess: docid to which the iterators must be moved to
     * @return -1 if there is at least a list for which there is no docid >= docidToProcess
     */
    private int moveIteratorsToDocid(int docidToProcess){
        // move the iterators for posting lists pointing to docids < docidToProcess
        for(int i=1; i<postingsToScore.size(); i++){
            // i-th posting list
            PostingList currPostingList = postingsToScore.get(i);

            // check if there are postings to iterate in the i-th posting list
            if(currPostingList != null){
                // I should move the iterator until I find a docid >= docidToProcess
                while(currPostingList.getPostings() != null && currPostingList.getPostings().get(0).getKey() < docidToProcess)
                    currPostingList.getPostings().remove(0);

                // check if in the current posting list there is no docid >= docidToProcess to be processed
                if(currPostingList.getPostings() == null){
                    return -1;
                }
            }
        }
        return docidToProcess;
    }

    /** method to find next document to be processed among all the postings to be scored
     * @return
     * - if query mode is DISJUNCTIVE, return the minimum docid among all the first docids in the posting lists of the array, -1 if not possible
     * - else, query mode is CONJUNCTIVE, return the maximum docid among all the first docids in the posting lists of the array,
     *      it returns -1 if the maximum docid is not present in all the posting lists to be scored
     * */
    private int nextDocToProcess(boolean isConjunctive){
        int docidToProcess = -1;

        // go through all the posting lists of other query terms
        for(int i=1; i<postingsToScore.size(); i++){
            // i-th posting list
            PostingList currPostingList = postingsToScore.get(i);

            // check if there are postings to iterate in the i-th posting list
            if(currPostingList != null && currPostingList.getPostings() != null){
                // retrieve docid of the first document in the pointed posting list
                int pointedDocid = currPostingList.getPostings().get(0).getKey();

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
        }

        if(isConjunctive)
            return moveIteratorsToDocid(docidToProcess);
        else
            return docidToProcess;
    }

    /**
     * method to compute the IDF score of a particular document identified by docid
     * @param docid: docid of the document to be scored
     * @return score of the document
     */
    private double scoreDocument(int docid){
        // initialization of document's score
        double docScore = 0;

        // find postings about the docid to be processed
        for(PostingList postingList : postingsToScore) {
            // check if the current postinglist is pointing to the docid we are currently processing

            if (postingList.getPostings() != null && postingList.getPostings().get(0).getKey() == docid) {
                // process the posting
                int tf = postingList.getPostings().get(0).getValue();
                double idf = 1; //TODO: get idf from vocabulary entry

                // adding tfidf to doc score
                docScore += ((1 + Math.log(tf)) * Math.log(idf));

                // posting scored, it can be removed by the postings to be scored
                postingList.getPostings().remove(0);
            }
        }
        return docScore;
    }

    /** method to process DAAT a list of posting list of the query terms using TFIDF as scoring function
     * @param queryPostings: list of postings of query terms
     * @param isConjuctive: if true, the query must be processed in CONJUNCTIVE way, else in DISJUNCTIVE way
     * @param k: number of top k documents to be returned
     * @return returns a priority queue (of at most K elements) in the format <SCORE (Double), DOCID (Integer)> ordered by increasing score value
     */
    public PriorityQueue<Map.Entry<Double, Integer>> scoreQuery(ArrayList<PostingList> queryPostings, boolean isConjuctive, int k){

        // avoid side effects
        postingsToScore = new ArrayList<>(queryPostings);

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());


        int docToProcess = nextDocToProcess(isConjuctive);

        // until there are documents to be processed
        while(docToProcess!= -1){
            double docScore = scoreDocument(docToProcess);

            // check if the MinHeap is full
            if(topKDocuments.size()==k){
                // MinHeap is full

                // check if the processed document can enter the MinHeap
                if (docScore > topKDocuments.peek().getKey()) {
                    // the current score enters the MinHeap

                    // remove the root of the MinHeap (the lowest score in top K documents)
                    topKDocuments.poll();

                    // insert the document and its score in the MinHeap
                    topKDocuments.add(new AbstractMap.SimpleEntry<>(docScore, docToProcess));
                }
            } else {
                // MinHeap is not full, the current document enters the MinHeap

                // insert the document and its score in the MinHeap
                topKDocuments.add(new AbstractMap.SimpleEntry<>(docScore, docToProcess));
            }

            // find next document to be processed
            docToProcess = nextDocToProcess(isConjuctive);
        }

        // TODO: change docid into pid
        return topKDocuments;
    }

}
