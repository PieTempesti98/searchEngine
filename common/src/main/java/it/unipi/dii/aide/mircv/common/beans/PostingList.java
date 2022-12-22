package it.unipi.dii.aide.mircv.common.beans;

import java.io.IOException;
import it.unipi.dii.aide.mircv.common.compression.UnaryCompressor;
import it.unipi.dii.aide.mircv.common.compression.VariableByteCompressor;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

public class PostingList{

    private String term;
    private final ArrayList<Posting> postings = new ArrayList<>();

    // variable used for computing the max dl to insert in the vocabulary
    private int maxDl = 0;

    public PostingList(String toParse) {
        String[] termRow = toParse.split("\t");
        this.term = termRow[0];
        if(termRow.length > 1)
            parsePostings(termRow[1]);
    }

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

    public void updateMaxDocumentLength(int length){
        if(length > this.maxDl)
            this.maxDl = length;
    }

    public void openList(){
        // TODO: implement method (Pietro)
        // load the block descriptors
    }

    public Posting next(){
        // TODO: implement method (Pietro)
        /*
            if !iterator(postings).hasNext
                loadBlock
                createNewIterator
            return iterator(postings).next
         */

        return new Posting();
    }


    /**
     * Returns last accessed posting
     * @return posting or null if there are no more postings.
     */
    public Posting getCurrentPosting(){
        //TODO: implement method (Pietro)

        return new Posting();
    }

    public Posting nextGEQ(int docid){
        // TODO: implement method (Pietro)
        /*
        while currentBlock.maxDocid < docid
            if !iterator(blocks).hasNext
                return null
            currentBlock = iterator(blocks).next
        while iterator(postings).hasNext
            currentPosting = iterator(posting).next
            if currentPosting.docid >= docid
                return currentPosting
         */
        return new Posting();
    }

    public void closeList(){
        //TODO: implement method (Pietro)

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
