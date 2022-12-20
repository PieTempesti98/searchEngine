package it.unipi.dii.aide.mircv.common.beans;

import java.io.IOException;
import it.unipi.dii.aide.mircv.common.compression.UnaryCompressor;
import it.unipi.dii.aide.mircv.common.compression.VariableByteCompressor;

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
        //TODO: must be block based
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

    /**
     * Save to disk the posting list:
     *  - write docids in docsPath
     *  - write freqs in freqsPath
     *  - update freqs and docs offsets (this generates side effects)
     * @param docsMemOffset: offset in which to write in docids file
     * @param freqsMemOffset: offset in which to write in freqs file
     * @param docsPath: file path for file storing docids
     * @param freqsPath: file path for file storing freqs
     * @param compressionMode: if true compression of docids and freqs is activated, else not
     * @return updated buffers positions of docids buffer and freqs buffers
     */
    public long[] writePostingListToDisk(long docsMemOffset, long freqsMemOffset, String docsPath, String freqsPath, boolean compressionMode) {
        // memory occupancy of the posting list:
        // - for each posting we have to store 2 integers (docid and freq)
        // - each integer will occupy 4 bytes since we are storing integers in byte arrays
        int[] numBytes = new int[2];
        byte[] compressedDocs = null;
        byte[] compressedFreqs = null;

        if(compressionMode){
            // compression is required
            int[] docids= new int[postings.size()];
            int[] freqs= new int[postings.size()];

            int nextPosting = 0;
            // initialize docids and freqs arrays
            for(Posting posting: postings){
                docids[nextPosting] = posting.getDocid();
                freqs[nextPosting] = posting.getFrequency();
            }

            compressedDocs = UnaryCompressor.integerArrayCompression(docids);
            compressedFreqs = VariableByteCompressor.integerArrayCompression(freqs);

            numBytes[0] = compressedDocs.length;
            numBytes[1] = compressedFreqs.length;

        }
        else{
            // compression not required
            // set docs num bytes as (number of postings)*4
            numBytes[0] = getNumBytes()/2;
            // set freqs num bytes as (number of postings)*4
            numBytes[1] = getNumBytes()/2;
        }

        // try to open a file channel to the file of the inverted index
        try (FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(docsPath), StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);
             FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(freqsPath), StandardOpenOption.WRITE,
                     StandardOpenOption.READ, StandardOpenOption.CREATE)
        ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docsMemOffset, numBytes[0]);

            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqsMemOffset, numBytes[1]);

            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null) {
                // write postings to file

                if(compressionMode){
                        docsBuffer.put(compressedDocs);
                        freqsBuffer.put(compressedFreqs);
                }
                else {
                    for (Posting posting : postings) {
                        // encode docid
                        docsBuffer.putInt(posting.getDocid());
                        // encode freq
                        freqsBuffer.putInt(posting.getFrequency());
                    }
                }

                /* DEBUG
                System.out.println("new offsets:\tdocs:"+(docsMemOffset + docsBuffer.position())+" freqs:"+(freqsMemOffset + freqsBuffer.position()));
                */

                //return updated buffer positions
                return new long[]{docsMemOffset+ docsBuffer.position(), freqsMemOffset + freqsBuffer.position()};
            }
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
        }
        return null;
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
}
