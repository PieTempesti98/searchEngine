package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.Posting;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;

import java.util.*;

public class MaxScore {

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


    /** method to process with MaxScore algorithm a list of posting list of the query terms
     * @param queryPostings: list of postings of query terms
     * @param vocEntries: list of vocabulary entries corresponding to posting lists of query terms
     * @param k: number of top k documents to be returned
     * @return returns a priority queue (of at most K elements) in the format <SCORE (Double), DOCID (Integer)> ordered by increasing score value
     */
    public static PriorityQueue<Map.Entry<Double, Integer>> scoreQuery(ArrayList<PostingList> queryPostings, ArrayList<VocabularyEntry> vocEntries, int k,String scoringFunction){

        System.out.println("query postings:\t"+ queryPostings);

        initialize(queryPostings);

        // deep copy of the posting lists to be scored (avoid side effects and modification of queryPostings
        //ArrayList<PostingList> copyPostings = deepCopy(queryPostings);

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        // sort by increasing term upper bound posting lists to be scored
        ArrayList<Map.Entry<PostingList, Double>> sortedLists = sortPostingListsByTermUpperBound(queryPostings, vocEntries, scoringFunction);

        // initialization of current threshold to enter the MinHeap of the results
        double currThreshold = -1;

        boolean currThresholdHasBeenUpdated = true;

        int firstEssentialPLIndex = 0;

        while(true){
            double partialScore = 0;
            double documentUpperBound = 0;

            // variable to store the sum of term upper bounds of non-essential posting lists
            double nonEssentialTUBs = 0;

            // check if we must update the division in essential and non-essential posting lists
            if(currThresholdHasBeenUpdated){
                // divide posting lists to be scored in essential and non-essential posting lists
                firstEssentialPLIndex = getFirstEssentialPostingListIndex(sortedLists, currThreshold);
            }

            // search for minimum docid to be scored among essential posting lists
            int docToProcess = nextDocToProcess(sortedLists, firstEssentialPLIndex);

            // check if there is no docid to be processed
            if(docToProcess == -1)
                break;

            // process DAAT the essential posting lists for docToProcess
            partialScore = processEssentialListsDAAT(sortedLists, firstEssentialPLIndex, docToProcess,scoringFunction);


            // sum the term upper bounds for all non-essential posting lists and save them in nonEssentialTUBs
            for(int i=0; i<firstEssentialPLIndex; i++){
                if(sortedLists.get(i)!=null)
                    nonEssentialTUBs += sortedLists.get(i).getValue();
            }

            // update document upper bound to PartialScore+sum(TermUpperBound of non-essential posting lists)
            documentUpperBound = partialScore + nonEssentialTUBs;

            // check if non-essential posting lists must be processed or not
            if(documentUpperBound > currThreshold){
                // process non-essential posting list skipping all documents up to docToProcess
                double nonEssentialScores = processNonEssentialListsWithSkipping(sortedLists, firstEssentialPLIndex, docToProcess,scoringFunction);

                // update document upper bound
                documentUpperBound = documentUpperBound -nonEssentialTUBs + nonEssentialScores;

                // check if the document can enter the MinHeap
                if(documentUpperBound>=currThreshold){
                    // the current document enters the MinHeap
                    // check if the MinHeap is full
                    if(topKDocuments.size()==k){
                        // MinHeap is full, remove the root of the MinHeap (the lowest score in top K documents)
                        topKDocuments.poll();
                    }

                    // insert the document and its score in the MinHeap
                    topKDocuments.add(new AbstractMap.SimpleEntry<>(documentUpperBound, docToProcess));

                    // update currentThreshold value if the MinHeap is full, else leave it with a value of -1
                    if(topKDocuments.size()==k)
                        currThreshold = documentUpperBound;
                }
            }

            // check if current threshold has been updated or not
            currThresholdHasBeenUpdated = (currThreshold == documentUpperBound);

        }

        System.out.println("top K:\t"+topKDocuments);
        cleanUp(queryPostings);
        return topKDocuments;
    }

    /**
     * get the scores for the input document, given by the non-essential posting list in the array list of the sorted lists
     * @param sortedLists : array list of the posting lists sorted by increasing term upper bound
     * @param firstEssentialPLIndex : index of the first essential-posting list
     * @param docToProcess : docid of the document to be processed
     * @param scoringFunction: scoring function to be used (tfidf or bm25)
     * @return double value corresponding to the partial score of docToProcess in the non-essential posting lists
     */
    private static double processNonEssentialListsWithSkipping(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex, int docToProcess, String scoringFunction) {
        double nonEssentialScore = 0;

        for(int i=0; i<firstEssentialPLIndex; i++){
            Map.Entry<PostingList, Double> postingList = sortedLists.get(i);

            Posting posting = postingList.getKey().nextGEQ(docToProcess);
            if(posting != null && posting.getDocid() == docToProcess) {
                // TODO: how to pass vocabulary entry
                nonEssentialScore += posting.scoreDocument(null, scoringFunction);
                postingList.getKey().next();
            }
        }

        return nonEssentialScore;
    }



    /**
     * given as input the posting lists sorted by term upper bound, the index of the first essential posting list,
     * and the docid of the document to be processed with DAAT, get the partial score of the document in the essential posting lists
     * @param sortedLists: posting lists sorted by increasing term upper bound
     * @param firstEssentialPLIndex: index of the first essential posting list
     * @param docToProcess: docid of doc to be processed DAAT in the essential posting lists
     * @return partial score given by essential posting lists for doc with docid equal to docToProcess
     */
    private static double processEssentialListsDAAT(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex, int docToProcess,String scoringFunction) {
        double partialScore = 0;

        // process essential lists
        for(int i=firstEssentialPLIndex; i<sortedLists.size(); i++){
            PostingList postingList = sortedLists.get(i).getKey();

            if(postingList == null)
                continue;

            Posting pointedPosting = postingList.getCurrentPosting();;

            if(pointedPosting != null){
                // check if minimum docid to be scored in current posting list is the one to be processed
                if(pointedPosting.getDocid() == docToProcess){

                    // process the current document
                    partialScore += pointedPosting.scoreDocument(null,scoringFunction);
                }
            }
        }
        return partialScore;
    }

    /**
     * Find which is the first essential posting list in the given array list according to the current threshold value given as input.
     * An essential posting list is a posting list whose term upper bound
     * summed up to the term upper bounds of the preceding posting lists is >= current threshold
     * @param sortedLists: array list of Map entries with the following format: <POSTING LIST><TERM UPPER BOUND>
     * @param currThreshold: current threshold to beat in order to enter the minHeap of the top k results
     * @return index of the first essential posting list (posting list that beats the threshold), -1 if no essential lsit was found
     */
    private static int getFirstEssentialPostingListIndex(ArrayList<Map.Entry<PostingList, Double>> sortedLists, double currThreshold) {
        int sumScores = 0;

        // scan all the posting lists
        for(int i=0; i<sortedLists.size(); i++){
            // sum the term upper bound of the current posting list to sumScores
            sumScores += sortedLists.get(i).getValue();

            // check if the sum of the term upper bounds up to now is greater equal than the current threshold

            if(sumScores>currThreshold)
                return i;
        }
        return -1;
    }

    /**
     * find the next term to be processed among the given input posting lists as the docid having min docid among the essential posting lists.
     * Notice that the search is performed only for posting lists having index >= firstEssentialPLIndex in the arrayList given as input.
     * @param sortedLists: sorted posting lists on which to perform the search
     * @param firstEssentialPLIndex: first index from which to search
     * @return integer corresponding to next docid to be processed, -1 if none
     */
    private static int nextDocToProcess(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex) {
        int nextDocid = -1;
        // TODO: handle also conjunctive queries
        // go through all posting list and search for minimum docid
        for(int i=firstEssentialPLIndex; i< sortedLists.size(); i++){

            Posting pointedPosting = sortedLists.get(i).getKey().getCurrentPosting();

            // if current posting  is not null and next docid is the curremt minimum
            if(pointedPosting!=null && (nextDocid==-1 || pointedPosting.getDocid() < nextDocid)){
                    nextDocid = pointedPosting.getDocid();
            }
        }
        return nextDocid;
    }

    /**
     * given the array of posting list of query terms and their vocabulary entries, sort them by increasing term upper bound
     * @param queryPostings: query posting lists to be sorted
     * @param vocabularyEntries: vocabulary entries of the given posting lists
     * @return arraylist of entries of the following format: <POSTING LIST><TERM UPPER BOUND>. The arraylist is sorted by increasing TUB
     */
    private static ArrayList<Map.Entry<PostingList, Double>> sortPostingListsByTermUpperBound(ArrayList<PostingList> queryPostings, ArrayList<VocabularyEntry> vocabularyEntries, String scoringFunction){
        // TODO: update how posting lists are sorted

        PriorityQueue<Map.Entry<PostingList, Double>> sortedPostingLists = new PriorityQueue<>(queryPostings.size(), Map.Entry.comparingByValue());

        for(int i=0; i< queryPostings.size(); i++){
            PostingList postingList = queryPostings.get(i);

            // retrieve document upper bound
            Double termUpperBound = null;
            if(scoringFunction.equals("tfidf")){
                termUpperBound = vocabularyEntries.get(i).getMaxTFIDF();
            }
            else
                termUpperBound= vocabularyEntries.get(i).getMaxBM25();

            sortedPostingLists.add(new AbstractMap.SimpleEntry<>(postingList, termUpperBound));
        }

        return new ArrayList<>(sortedPostingLists.stream().toList());
    }
}
