package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.VocabularyEntry;

import java.util.*;

public class MaxScore {
    /** method to process with MaxScore algorithm a list of posting list of the query terms
     * @param queryPostings: list of postings of query terms
     * @param vocEntries: list of vocabulary entries corresponding to posting lists of query terms
     * @param k: number of top k documents to be returned
     * @return returns a priority queue (of at most K elements) in the format <SCORE (Double), DOCID (Integer)> ordered by increasing score value
     */
    public static PriorityQueue<Map.Entry<Double, Integer>> scoreQuery(ArrayList<PostingList> queryPostings, ArrayList<VocabularyEntry> vocEntries, int k){

        System.out.println("query postings:\t"+ queryPostings);

        // deep copy of the posting lists to be scored (avoid side effects and modification of queryPostings
        ArrayList<PostingList> copyPostings = deepCopy(queryPostings);

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        // sort by increasing term upper bound posting lists to be scored
        ArrayList<Map.Entry<PostingList, Double>> sortedLists = sortPostingListsByTermUpperBound(copyPostings, vocEntries);

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
            partialScore = processEssentialListsDAAT(sortedLists, firstEssentialPLIndex, docToProcess);


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
                double nonEssentialScores = processNonEssentialListsWithSkipping(sortedLists, firstEssentialPLIndex, docToProcess);

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

            moveToNextDocid(sortedLists, docToProcess);
        }

        System.out.println("top K:\t"+topKDocuments);
        return topKDocuments;
    }

    /**
     * get the scores for the input document, given by the non-essential posting list in the array list of the sorted lists
     * @param sortedLists: array list of the posting lists sorted by increasing term upper bound
     * @param docToProcess: docid of the document to be processed
     * @param firstEssentialPLIndex: index of the first essential-posting list
     * @return double value corresponding to the partial score of docToProcess in the non-essential posting lists
     */
    private static double processNonEssentialListsWithSkipping(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex,  int docToProcess) {
        double nonEssentialScore = 0;
        // TODO: implement

        for(int i=0; i<firstEssentialPLIndex; i++){
            Map.Entry<PostingList, Double> postingList = sortedLists.get(i);

            Map.Entry<Integer, Integer> posting = postingList.skipToDocid(docToProcess);
            // TODO: skip to docToProcess
            nonEssentialScore += postingList.getKey().scoreDocument();
        }

        return nonEssentialScore;
    }

    /**
     * Given as input the array list of the posting lists sorted by increasing term upper bound and the docid of the processed document,
     * update the sorted lists in such a way that they will be pointing to nextGEQ(lastProcessedDocid)
     * @param sortedLists: array list of the posting lists sorted by increasing term upper bound
     * @param lastProcessedDocid: last processed docid
     */
    private static void moveToNextDocid(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int lastProcessedDocid) {

        // TODO: next operation on posting lists

        for(Map.Entry<PostingList, Double> sortedEntry : sortedLists){
            PostingList postingList = sortedEntry.getKey();

            if(postingList!=null){
                ArrayList<Map.Entry<Integer, Integer>> postings = postingList.getPostings();

                // check if in the current posting list there is the processed docid
                if(postings != null && postings.get(0)!=null && postings.get(0).getKey() == lastProcessedDocid){
                    // move to next posting the current posting list
                    postings.remove(0);
                }
            }
        }
    }

    /**
     * perform the deep copy of the given arraylist of posting lists
     * @param queryPostings: posting lists to be copied
     * @return an arraylist of posting lists equal to the one given as input
     */
    private static ArrayList<PostingList> deepCopy(ArrayList<PostingList> queryPostings) {
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
        return copyPostings;
    }

    /**
     * given as input the posting lists sorted by term upper bound, the index of the first essential posting list,
     * and the docid of the document to be processed with DAAT, get the partial score of the document in the essential posting lists
     * @param sortedLists: posting lists sorted by increasing term upper bound
     * @param firstEssentialPLIndex: index of the first essential posting list
     * @param docToProcess: docid of doc to be processed DAAT in the essential posting lists
     * @return partial score given by essential posting lists for doc with docid equal to docToProcess
     */
    private static double processEssentialListsDAAT(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex, int docToProcess) {
        double partialScore = 0;

        // process essential lists
        for(int i=firstEssentialPLIndex; i<sortedLists.size(); i++){
            PostingList postingList = sortedLists.get(i).getKey();

            if(postingList == null)
                continue;

            ArrayList<Map.Entry<Integer, Integer>> postings = postingList.getPostings();

            if(postings != null && postings.get(0)!=null){
                // check if minimum docid to be scored in current posting list is the one to be processed
                if(postings.get(0).getKey() == docToProcess){

                    // process the current document
                    partialScore += scoreDocument();
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

        // go through all posting list and search for minimum docid
        for(int i=firstEssentialPLIndex; i< sortedLists.size(); i++){
            ArrayList<Map.Entry<Integer, Integer>> postings = sortedLists.get(i).getKey().getPostings();

            // if current posting list is not empty
            if(postings!=null && !postings.isEmpty() && postings.get(0)!=null){
                // search for minimum
                if(nextDocid==-1 || postings.get(0).getKey() < nextDocid)
                    nextDocid = postings.get(0).getKey();
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
    private static ArrayList<Map.Entry<PostingList, Double>> sortPostingListsByTermUpperBound(ArrayList<PostingList> queryPostings, ArrayList<VocabularyEntry> vocabularyEntries){

        // TODO: verify that term upper bounds of posting lists are already computed

        // TODO: update how posting lists are sorted

        PriorityQueue<Map.Entry<PostingList, Double>> sortedPostingLists = new PriorityQueue<>(queryPostings.size(), Map.Entry.comparingByValue());

        for(int i=0; i< queryPostings.size(); i++){
            PostingList postingList = queryPostings.get(i);
            Double termUpperBound = vocabularyEntries.get(i).getTermUpperBound();
            sortedPostingLists.add(new AbstractMap.SimpleEntry<>(postingList, termUpperBound));
        }

        return new ArrayList<>(sortedPostingLists.stream().toList());
    }
}
