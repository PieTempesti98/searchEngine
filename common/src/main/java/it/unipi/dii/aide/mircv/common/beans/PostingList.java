package it.unipi.dii.aide.mircv.common.beans;

import java.io.IOException;
import it.unipi.dii.aide.mircv.common.compression.UnaryCompressor;
import it.unipi.dii.aide.mircv.common.compression.VariableByteCompressor;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
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
    private int maxDl = 0;

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
     * Constructor for PostingList
     * it reads the PostingList directly from file
     * @param term: vocabulary entry for the posting list to be constructed
     * @param docsPath: file path for the file containing docids of the posting list to be constructed
     * @param freqsPath: file path for the file containing freqs of the posting list to be constructed
     */
    public PostingList(VocabularyEntry term, String docsPath, String freqsPath) {

        /*
        DEBUG
        System.out.println("reading posting list for term:"+term.getTerm());
*/
        // TODO: relocate and reuse filechannels

        try (FileChannel docsFChan = (FileChannel) Files.newByteChannel(
                Paths.get(docsPath),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE);
             FileChannel freqsFChan = (FileChannel) Files.newByteChannel(
                     Paths.get(freqsPath),
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.CREATE)
        ) {
            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docBuffer = docsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getDocidOffset(),
                    term.getDocidSize()
            );

            // instantiation of MappedByteBuffer for integer list of frequencies
            MappedByteBuffer freqBuffer = freqsFChan.map(
                    FileChannel.MapMode.READ_ONLY,
                    term.getFrequencyOffset(),
                    term.getFrequencySize()
            );

            // create the posting list for the term
            this.term = term.getTerm();

            for (int i = 0; i < term.getDf(); i++) {
                Posting posting = new Posting(docBuffer.getInt(), freqBuffer.getInt());
                this.getPostings().add(posting);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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
     * method to return the numbers of bytes occupied by this posting list when stored in memory
     * @return posting list's space occupancy in bytes
     */
    public int getNumBytes() {
        return postings.size()*4*2;
    }
    @Override
    public String toString() {
        return "PostingList{" +
                "term='" + term + '\'' +
                ", postings=" + postings +
                '}';
    }

    /**
     * Update the max document length
     * @param length the candidate max document length
     */
    public void updateMaxDocumentLength(int length){
        if(length > this.maxDl)
            this.maxDl = length;
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
            if (!blocksIterator.hasNext())
                return null;
            // load the new block and update the postings iterator
            currentBlock = blocksIterator.next();
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
            if(!blocksIterator.hasNext())
                return null;
            currentBlock = blocksIterator.next();
            blockChanged = true;
        }
        // block changed, load postings and update iterator
        if(blockChanged){
            postings.addAll(currentBlock.getBlockPostings());
            postingIterator = postings.iterator();
        }
        // move to the first GE posting and return it
        while(postingIterator.hasNext()){
            currentPosting = postingIterator.next();
            if (currentPosting.getDocid() >= docid)
                return currentPosting;
        }
        return null;
    }

    public void closeList(){

        // clear the list of postings
        postings.clear();

        // clear the list of blocks
        blocks.clear();

        // remove the term from the vocabulary
        Vocabulary.getInstance().remove(term);
    }

    /**
     * function to write the posting list as plain text in the debug files
     * @param docidsPath: path of docids file where to write
     * @param freqsPath: path of freqs file where to write
     */
    public void debugSaveToDisk(String docidsPath, String freqsPath){
        FileUtils.createDirectory("data/debug");
        FileUtils.createIfNotExists("data/debug/"+docidsPath);
        FileUtils.createIfNotExists("data/debug/"+freqsPath);

        try {
            BufferedWriter writerDocids = new BufferedWriter(new FileWriter("data/debug/"+docidsPath, true));
            BufferedWriter writerFreqs = new BufferedWriter(new FileWriter("data/debug/"+freqsPath, true));
            for(Posting p: postings){
                writerDocids.write(p.getDocid()+" ");
                writerFreqs.write(p.getFrequency()+" ");
            }
            writerDocids.write("\n");
            writerFreqs.write("\n");

            writerDocids.close();
            writerFreqs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
