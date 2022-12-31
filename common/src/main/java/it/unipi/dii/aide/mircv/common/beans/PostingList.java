package it.unipi.dii.aide.mircv.common.beans;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class PostingList{

    /**
     * the term of the posting list
     */
    private String term;

    /**
     * the list of the postings loaded in memory
     */
    private final ArrayList<Posting> postings = new ArrayList<>();

    /**
     * the list of the blocks n which the posting list is divided
     */
    private ArrayList<BlockDescriptor> blocks = null;

    /**
     * iterator for the postings
     */
    private Iterator<Posting> postingIterator = null;

    /**
     * iterator for the blocks
     */
    private Iterator<BlockDescriptor> blocksIterator = null;

    /**
     * the current block
     */
    private BlockDescriptor currentBlock = null;

    /**
     * the current posting
     */
    private Posting currentPosting = null;

    /**
     * variable used for computing the max dl to insert in the vocabulary
     */
    private int BM25Dl = 1;

    private int BM25Tf = 0;

    /**
     * constructor that create a posting list from a string
     * @param toParse the string from which we can parse the posting list, with 2 formats:
     *                <ul>
     *                <li>[term] -> only the posting term</li>
     *                <li>[term] \t [docid]:[frequency] [docid]:{frequency] ... -> the term and the posting list}</li>
     *                </ul>
     */
    public PostingList(String toParse) {
        String[] termRow = toParse.split("\t");
        this.term = termRow[0];
        if(termRow.length > 1)
            parsePostings(termRow[1]);
    }

    /**
     * default constructor
     */
    public PostingList(){}

    /**
     * parses the postings from a string
     * @param rawPostings string with the rea postings
     */
    private void parsePostings(String rawPostings){
        String[] documents = rawPostings.split(" ");
        for(String elem: documents){
            String[] posting = elem.split(":");
            postings.add(new Posting(Integer.parseInt(posting[0]), Integer.parseInt(posting[1])));
        }
    }

    public String getTerm() {
        return term;
    }

    public ArrayList<Posting> getPostings() {
        return postings;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public void appendPostings(ArrayList<Posting> newPostings){
        postings.addAll(newPostings);
    }

    /**
     * Update the max document length
     * @param length the candidate max document length
     */
    public void updateBM25Parameters(int length, int tf){
        double currentRatio = (double) this.BM25Tf / (double) (this.BM25Dl + this.BM25Tf);
        double newRatio = (double) tf / (double) (length + tf);
        if(newRatio > currentRatio){
            this.BM25Tf = tf;
            this.BM25Dl = length;
        }
    }

    /**
     * method that opens and initializes the posting list for the query processing
     */
    public void openList(){
        // load the block descriptors
        blocks = Vocabulary.getInstance().get(term).readBlocks();

        // return false if the blocks are not loaded
        if(blocks == null){
            return;
        }

        // initialize block iterator
        blocksIterator = blocks.iterator();

        //initialize postings iterator
        postingIterator = postings.iterator();

    }

    /**
     * returns the next posting in the list
     * @return the next posting in the list
     */
    public Posting next(){
        // no postings in memory: load new block
        if(!postingIterator.hasNext()) {

            // no new blocks: end of list
            if (!blocksIterator.hasNext()) {
                currentPosting = null;
                return null;
            }

            // load the new block and update the postings iterator
            currentBlock = blocksIterator.next();
            //remove previous postings
            postings.clear();
            postings.addAll(currentBlock.getBlockPostings());
            postingIterator = postings.iterator();
        }
        // return the next posting to process
        currentPosting = postingIterator.next();
        return currentPosting;
    }


    /**
     * Returns last accessed posting
     * @return posting or null if there are no more postings.
     */
    public Posting getCurrentPosting(){
        return currentPosting;
    }

    /**
     * returns the first posting with docid greater or equal than the specified docid.
     * If there's no greater or equal docid in the list returns null
     * @param docid the docid to reach in the list
     * @return the first posting with docid greater or equal than the specified docid, null if this posting doesn't exist
     */
    public Posting nextGEQ(int docid){
        // flag to check if the block has changed
        boolean blockChanged = false;
        // move to the block with max docid >= docid
        // current block is null only if it's the first read
        while(currentBlock == null || currentBlock.getMaxDocid() < docid){
            // end of list, return null
            if(!blocksIterator.hasNext()){
                currentPosting = null;
                return null;
            }

            currentBlock = blocksIterator.next();
            blockChanged = true;
        }
        // block changed, load postings and update iterator
        if(blockChanged){
            //remove previous postings
            postings.clear();
            postings.addAll(currentBlock.getBlockPostings());
            postingIterator = postings.iterator();
        }
        // move to the first GE posting and return it
        while(postingIterator.hasNext()){
            currentPosting = postingIterator.next();
            if (currentPosting.getDocid() >= docid)
                return currentPosting;
        }
        currentPosting = null;
        return currentPosting;
    }

    public void closeList(){

        // clear the list of postings
        postings.clear();

        // clear the list of blocks
        blocks.clear();

        // remove the term from the vocabulary
        Vocabulary.getInstance().remove(term);
    }
    
    // TODO: fix posting list toString()

    /**
     * function to write the posting list as plain text in the debug files
     * @param docidsPath: path of docids file where to write
     * @param freqsPath: path of freqs file where to write
     */
    public void debugSaveToDisk(String docidsPath, String freqsPath, int maxPostingsPerBlock){
        FileUtils.createDirectory("data/debug");
        FileUtils.createIfNotExists("data/debug/"+docidsPath);
        FileUtils.createIfNotExists("data/debug/"+freqsPath);
        FileUtils.createIfNotExists("data/debug/completeList.txt");

        try {
            BufferedWriter writerDocids = new BufferedWriter(new FileWriter("data/debug/"+docidsPath, true));
            BufferedWriter writerFreqs = new BufferedWriter(new FileWriter("data/debug/"+freqsPath, true));
            BufferedWriter all = new BufferedWriter(new FileWriter("data/debug/completeList.txt", true));
            String[] postingInfo = toStringPosting();
            int postingsPerBlock = 0;
            for(Posting p: postings){
                writerDocids.write(p.getDocid()+" ");
                writerFreqs.write(p.getFrequency()+" ");
                postingsPerBlock ++;
                // check if I reach the maximum number of terms per block
                if(postingsPerBlock == maxPostingsPerBlock){
                    // write the block separator on file
                    writerDocids.write("| ");
                    writerFreqs.write("| ");

                    // reset tne number of postings to zero
                    postingsPerBlock = 0;
                }
            }
            writerDocids.write("\n");
            writerFreqs.write("\n");

            writerDocids.write(postingInfo[0] + "\n");
            writerFreqs.write(postingInfo[1] + "\n");
            all.write(this.toString());
            writerDocids.close();
            writerFreqs.close();
            all.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String[] toStringPosting() {

        StringBuilder resultDocids = new StringBuilder();
        StringBuilder resultFreqs = new StringBuilder();

        resultDocids.append(term).append(" -> ");
        resultFreqs.append(term).append(" -> ");

        int curBlock = 0;
        int curPosting = 0;
        int numPostings = postings.size();
        int numBlocks = 1;

        if(postings.size() > 1024) {
            numBlocks = (int) Math.ceil(Math.sqrt(postings.size()));
            numPostings = (int) Math.ceil( postings.size() / (double) numBlocks);
        }

        while(curBlock < numBlocks){

            //The number of postings in the last block may be greater from the actual number of postings it contains
            int n = Math.min(numPostings,postings.size() - curPosting);

            for(int i = 0; i < n; i++){
                resultDocids.append(postings.get(curPosting).getDocid());
                resultFreqs.append(postings.get(curPosting).getFrequency());

                if(i != n - 1) {
                    resultDocids.append(", ");
                    resultFreqs.append(", ");
                }
                curPosting++;
            }

            curBlock++;

            //there are iterations left
            if(curBlock != numBlocks ) {
                resultDocids.append(" | ");
                resultFreqs.append(" | ");
            }
        }
        return new String[]{resultDocids.toString(),resultFreqs.toString()};
    }



    @Override
    public String toString() {

        StringBuilder result = new StringBuilder();
        result.append("\"");
        result.append(term);
        result.append('\t');
        for(Posting p: postings){
            result.append(p.getDocid()).append(":").append(p.getFrequency()).append(" ");
        }
        result.append("\"");
        result.append('\n');

        return result.toString();
    }

    public int getBM25Dl() {
        return BM25Dl;
    }

    public int getBM25Tf(){
        return BM25Tf;
    }

    public void setBM25Dl(int BM25Dl) {this.BM25Dl = BM25Dl;}

    public void setBM25Tf(int BM25Tf) {this.BM25Tf = BM25Tf;}
}
