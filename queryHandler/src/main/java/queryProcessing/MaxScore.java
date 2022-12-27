package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.Posting;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.Vocabulary;

import java.util.*;

public class MaxScore {

    /**
     * method to open and to perform the first "next()" operation on posting lists to be initialized to be then scored
     * @param queryPostings: posting lists to be initialized
     */
    private static void initialize(ArrayList<PostingList> queryPostings){

        for(PostingList postingList: queryPostings) {
            postingList.openList();
            postingList.next();
        }

    }

    /**
     * method to close the posting lists after the computations ended
     * @param queryPostings: posting lists to be closed
     */
    private static void cleanUp(ArrayList<PostingList> queryPostings) {

        for(PostingList postingList: queryPostings)
            postingList.closeList();

    }


    /** method to process with MaxScore algorithm a list of posting list of the query terms
     * @param queryPostings: list of postings of query terms
     * @param k: number of top k documents to be returned
     * @return returns a priority queue (of at most K elements) in the format <SCORE (Double), DOCID (Integer)> ordered by increasing score value
     */
    public static PriorityQueue<Map.Entry<Double, Integer>> scoreQuery(ArrayList<PostingList> queryPostings, int k,String scoringFunction, boolean conjunctiveMode){

        //System.out.println("query postings:\t"+ queryPostings);

        initialize(queryPostings);

        // deep copy of the posting lists to be scored (avoid side effects and modification of queryPostings
        //ArrayList<PostingList> copyPostings = deepCopy(queryPostings);

        // initialization of the MinHeap for the results
        PriorityQueue<Map.Entry<Double, Integer>> topKDocuments = new PriorityQueue<>(k, Map.Entry.comparingByKey());

        // sort by increasing term upper bound posting lists to be scored
        ArrayList<Map.Entry<PostingList, Double>> sortedLists = sortPostingListsByTermUpperBound(queryPostings, scoringFunction);

        /* DEBUG
        System.out.println("sorted lists:");

        for(Map.Entry<PostingList, Double> i : sortedLists){
            System.out.println("posting list:" + i.getKey() + " tub:"+i.getValue());
        }
        */

        // initialization of current threshold to enter the MinHeap of the results
        double currThreshold = -1;

        boolean currThresholdHasBeenUpdated = true;

        int firstEssentialPLIndex = 0;
        int iteration=0;
        while(true){
            iteration++;
            if(iteration==12)
                break;
            //System.out.println("iteration: "+iteration);
            double partialScore = 0;
            double documentUpperBound = 0;
            //System.out.println("new iteration with curr_threshold: "+currThreshold);

            // variable to store the sum of term upper bounds of non-essential posting lists
            double nonEssentialTUBs = 0;

            // check if we must update the division in essential and non-essential posting lists
            if(currThresholdHasBeenUpdated){
                // divide posting lists to be scored in essential and non-essential posting lists
                firstEssentialPLIndex = getFirstEssentialPostingListIndex(sortedLists, currThreshold);
                //System.out.println("first essential PL index: "+firstEssentialPLIndex);
                if(firstEssentialPLIndex==-1){
                    //System.out.println("no essential PL found, stop");

                    /* DEBUG
                    for(Map.Entry<PostingList, Double> i: sortedLists){
                        PostingList p = i.getKey();

                        if(p.getCurrentPosting()!=null){
                            System.out.println("posting: ("+p.getCurrentPosting().getDocid()+","+p.getCurrentPosting().getFrequency()+") score: "+Scorer.scoreDocument(p.getCurrentPosting(), Vocabulary.getInstance().getIdf(i.getKey().getTerm()), scoringFunction));
                        }

                        while(p.next()!=null){
                            if(p.getCurrentPosting()!=null){
                                System.out.println("posting: ("+p.getCurrentPosting().getDocid()+","+p.getCurrentPosting().getFrequency()+") score: "+Scorer.scoreDocument(p.getCurrentPosting(), Vocabulary.getInstance().getIdf(i.getKey().getTerm()), scoringFunction));
                            }
                        }
                    }
                    */
                    break;
                }
            }

            // search for minimum docid to be scored among essential posting lists
            int docToProcess = nextDocToProcess(sortedLists, firstEssentialPLIndex, conjunctiveMode);

            //System.out.println("next doc to process: "+docToProcess);

            // check if there is no docid to be processed
            if(docToProcess == -1)
                break;

            if(conjunctiveMode){
                docToProcess = nextGEQ(sortedLists, docToProcess);
            }

            // process DAAT the essential posting lists for docToProcess
            partialScore = processEssentialListsDAAT(sortedLists, firstEssentialPLIndex, docToProcess,scoringFunction);

            //System.out.println("essential partial score: "+partialScore);


            // sum the term upper bounds for all non-essential posting lists and save them in nonEssentialTUBs
            for(int i=0; i<firstEssentialPLIndex; i++){
                if(sortedLists.get(i)!=null)
                    nonEssentialTUBs += sortedLists.get(i).getValue();
            }

            //System.out.println("non essential tubs: "+nonEssentialTUBs);

            // update document upper bound to PartialScore+sum(TermUpperBound of non-essential posting lists)
            documentUpperBound = partialScore + nonEssentialTUBs;

            //System.out.println("document upper bound: "+documentUpperBound);

            // check if non-essential posting lists must be processed or not
            if(documentUpperBound > currThreshold){
                //System.out.println("beats the threshold");
                // process non-essential posting list skipping all documents up to docToProcess
                double nonEssentialScores = processNonEssentialListsWithSkipping(sortedLists, firstEssentialPLIndex, docToProcess,scoringFunction);

                //System.out.println("non essential score: "+nonEssentialScores);
                // update document upper bound
                documentUpperBound = documentUpperBound -nonEssentialTUBs + nonEssentialScores;

                //System.out.println("actual score for document: "+documentUpperBound);
                // check if the document can enter the MinHeap
                if(documentUpperBound>currThreshold){
                    //System.out.println("beats the threshold");
                    // the current document enters the MinHeap
                    // check if the MinHeap is full
                    if(topKDocuments.size()==k){
                        //System.out.println("heap full");
                        // MinHeap is full, remove the root of the MinHeap (the lowest score in top K documents)
                        topKDocuments.poll();
                    }

                    //System.out.println("inserted "+docToProcess);

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

        //System.out.println("top K:\t"+topKDocuments);
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

            if(postingList.getKey().getCurrentPosting()!= null && postingList.getKey().getCurrentPosting().getDocid()==docToProcess){
                //System.out.println("GEQ in list "+i+" is current docid");
                nonEssentialScore += Scorer.scoreDocument(postingList.getKey().getCurrentPosting(), Vocabulary.getInstance().getIdf(postingList.getKey().getTerm()), scoringFunction);
                postingList.getKey().next();
                continue;
            }

            Posting posting = postingList.getKey().nextGEQ(docToProcess);
            if(posting != null && posting.getDocid() == docToProcess) {
                //System.out.println("GEQ in list "+i+" : ("+posting.getDocid()+","+posting.getFrequency()+")");
                nonEssentialScore += Scorer.scoreDocument(posting, Vocabulary.getInstance().getIdf(postingList.getKey().getTerm()), scoringFunction);
                postingList.getKey().next();
            }/*
            else
            {
                if(posting != null)
                    System.out.println("GEQ in list "+i+" : ("+posting.getDocid()+","+posting.getFrequency()+")");
                else
                    System.out.println("no GEQ in list: "+i);
            }
*/
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

            Posting pointedPosting = postingList.getCurrentPosting();

            if(pointedPosting != null){
                // check if minimum docid to be scored in current posting list is the one to be processed
                if(pointedPosting.getDocid() == docToProcess){
                    //System.out.println("scoring term:"+postingList.getTerm());
                    // process the current document
                    partialScore += Scorer.scoreDocument(pointedPosting, Vocabulary.getInstance().getIdf(postingList.getTerm()), scoringFunction);
                    postingList.next();
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
     * @return index of the first essential posting list (posting list that beats the threshold), -1 if no essential list was found
     */
    private static int getFirstEssentialPostingListIndex(ArrayList<Map.Entry<PostingList, Double>> sortedLists, double currThreshold) {
        double sumScores = 0;

        // scan all the posting lists
        for(int i=0; i<sortedLists.size(); i++){
            //System.out.println("list " +i+" has a tub of: " + sortedLists.get(i).getValue());

            if(sortedLists.get(i).getKey().getCurrentPosting()==null){
                //System.out.println("no documents to be processed in list: "+i);
                continue;
            }

            // sum the term upper bound of the current posting list to sumScores
            sumScores = sumScores + sortedLists.get(i).getValue();

            //System.out.println("sum scores:" + sumScores);

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
     * @param conjunctiveMode: if true, conjunctive mode is enabled (maximum is returned), else disjunctive mode is enabled (minimum is returned)
     * @return integer corresponding to next docid to be processed (maximum if conjunctive mode is true, minimum else), -1 if none
     */
    private static int nextDocToProcess(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int firstEssentialPLIndex, boolean conjunctiveMode) {
        int nextDocid = -1;

        // go through all posting list and search for minimum docid
        for(int i=firstEssentialPLIndex; i< sortedLists.size(); i++){

            Posting pointedPosting = sortedLists.get(i).getKey().getCurrentPosting();

            if(pointedPosting==null)
                continue;

            if(conjunctiveMode){
                // if current posting  is not null and next docid is the current minimum
                if(nextDocid == -1 || pointedPosting.getDocid() > nextDocid){
                    nextDocid = pointedPosting.getDocid();
                }
            }
            else{
                // if current posting  is not null and next docid is the current minimum
                if(nextDocid == -1 || pointedPosting.getDocid() < nextDocid){
                    nextDocid = pointedPosting.getDocid();
                }
            }

        }
        return nextDocid;
    }


    /** method to move the iterators of postingsToScore to the given docid
     * @param sortedLists: posting lists that must be moved towards the given docid
     * @param docidToProcess: docid to which the iterators must be moved to
     * @return -1 if there is at least a list for which there is no docid > docidToProcess
     */
    private static int nextGEQ(ArrayList<Map.Entry<PostingList, Double>> sortedLists, int docidToProcess){
        // move the iterators for posting lists pointing to docids < docidToProcess

        //System.out.println("moving iterators towards docid: \t"+docidToProcess);

        int nextGEQ = docidToProcess;

        for(int i=0; i<sortedLists.size(); i++){
            // i-th posting list
            PostingList currPostingList = sortedLists.get(i).getKey();

            // check if there are postings to iterate in the i-th posting list
            if(currPostingList != null){
                Posting pointedPosting = currPostingList.getCurrentPosting();

                if(pointedPosting == null){
                    //System.out.println("conjunctive mode end");
                    return -1;
                }

                if(pointedPosting.getDocid() < nextGEQ) {
                    pointedPosting = currPostingList.nextGEQ(nextGEQ);
                    // check if in the current posting list there is no docid >= docidToProcess to be processed
                    if(pointedPosting == null){
                        //System.out.println("conjunctive mode end");
                        return -1;
                    }
                }

                // check if in the current posting list is not present docidToProcess, but it is present a docid >
                if (pointedPosting.getDocid()>nextGEQ) {
                    // the current docid will be the candidate next docid to be processed

                    // set nextGEQ to new value
                    nextGEQ = pointedPosting.getDocid();
                    i=-1;
                    //System.out.println("nextGEQ updated to"+nextGEQ);

                }
            }
        }

        //System.out.println("conjunctive mode, next docid to be processed:\t"+docidToProcess);
        return nextGEQ;
    }

    /**
     * given the array of posting list of query terms and their vocabulary entries, sort them by increasing term upper bound
     * @param queryPostings: query posting lists to be sorted
     * @return arraylist of entries of the following format: <POSTING LIST><TERM UPPER BOUND>. The arraylist is sorted by increasing TUB
     */
    private static ArrayList<Map.Entry<PostingList, Double>> sortPostingListsByTermUpperBound(ArrayList<PostingList> queryPostings, String scoringFunction){
        PriorityQueue<Map.Entry<PostingList, Double>> sortedPostingLists = new PriorityQueue<>(queryPostings.size(), Map.Entry.comparingByValue());

        for (PostingList postingList : queryPostings) {
            // retrieve document upper bound
            double termUpperBound;
            if (scoringFunction.equals("tfidf")) {
                termUpperBound = Vocabulary.getInstance().get(postingList.getTerm()).getMaxTFIDF();
            } else
                termUpperBound = Vocabulary.getInstance().get(postingList.getTerm()).getMaxBM25();

            //System.out.println("term upper bound for pl " + queryPostings.indexOf(postingList)+": "+termUpperBound);

            sortedPostingLists.add(new AbstractMap.SimpleEntry<>(postingList, termUpperBound));
        }

        return new ArrayList<>(sortedPostingLists.stream().toList());
    }
}
