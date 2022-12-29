package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import java.io.*;
import java.io.BufferedReader;
import java.io.IOException;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

public class Spimi {

    /**
     * path to the file on the disk storing the processed collection
     */
    private static final String PATH_TO_COLLECTION = ConfigurationParameters.getRawCollectionPath();

    /**
     * path to the file on the disk storing the compressed collection
     */
    protected static final String PATH_COMPRESSED_COLLECTION = ConfigurationParameters.getCompressedCollectionPath();

    /*
    path to the file on the disk storing the partial vocabulary
    */
    private static final String PATH_TO_PARTIAL_VOCABULARY = ConfigurationParameters.getPartialVocabularyDir() + ConfigurationParameters.getVocabularyFileName();

    /*
    path to the file on the disk storing the partial frequencies of the posting list
    */
    private static final String PATH_TO_PARTIAL_FREQUENCIES = ConfigurationParameters.getFrequencyDir() + ConfigurationParameters.getFrequencyFileName();

    /*
    path to the file on the disk storing the partial docids of the posting list
    */

    private static final String PATH_TO_PARTIAL_DOCID = ConfigurationParameters.getDocidsDir() + ConfigurationParameters.getDocidsFileName();


    /*
    counts the number of partial indexes created
     */
    private static int numIndex = 0;

    /*
    counts the number of partial indexes to write
     */
    private static long numPostings = 0;

    /**
     * @param compressed  flag for compressed reading
     * @return buffer reader
     * initializes the buffer from which the entries are read
     * */
    public static BufferedReader initBuffer(boolean compressed) throws IOException {

        if(compressed) { //read from compressed collection
            TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(PATH_COMPRESSED_COLLECTION)));
            tarInput.getNextTarEntry();
            return new BufferedReader(new InputStreamReader(tarInput, StandardCharsets.UTF_8));
        }

        return Files.newBufferedReader(Paths.get(PATH_TO_COLLECTION), StandardCharsets.UTF_8);

    }


    /**
     * deletes directories containing partial data structures and document Index file
     */
    private static void rollback(){

        FileUtils.deleteDirectory(ConfigurationParameters.getDocidsDir());
        FileUtils.deleteDirectory(ConfigurationParameters.getFrequencyDir());
        FileUtils.deleteDirectory(ConfigurationParameters.getPartialVocabularyDir());
        FileUtils.removeFile(ConfigurationParameters.getDocumentIndexPath());
    }

    /**
     * @param index: partial index that must be saved onto file
     */
    private static boolean saveIndexToDisk(HashMap<String, PostingList> index, boolean debugMode) {
        System.out.println("saving index: "+numIndex+" of size: "+index.size());

        if (index.isEmpty()){
            //if the index is empty there is nothing to write on disk
            System.out.println("empty index");
            return true;
        }


        //sort index in lexicographic order
        index = index.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        // try to open a file channel to the file of the inverted index
        try (
                FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_DOCID + "_" + numIndex),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_FREQUENCIES + "_" + numIndex),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE);
                FileChannel vocabularyFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_VOCABULARY + "_" + numIndex),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE)
        ) {

            // instantiation of MappedByteBuffer for integer list of docids
            MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPostings * 4L);

            // instantiation of MappedByteBuffer for integer list of freqs
            MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, 0, numPostings * 4L);

            long vocOffset = 0;
            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null) {
                for (PostingList list : index.values()) {
                    //create vocabulary entry
                    VocabularyEntry vocEntry = new VocabularyEntry(list.getTerm());
                    vocEntry.setMemoryOffset(docsBuffer.position());
                    vocEntry.setFrequencyOffset(docsBuffer.position());

                    // write postings to file
                    for (Posting posting : list.getPostings()) {
                        // encode docid
                        docsBuffer.putInt(posting.getDocid());
                        // encode freq
                        freqsBuffer.putInt(posting.getFrequency());
                    }
                    vocEntry.updateStatistics(list);
                    vocEntry.setBM25Dl(list.getBM25Dl());
                    vocEntry.setBM25Tf(list.getBM25Tf());
                    vocEntry.setDocidSize((int) (numPostings*4));
                    vocEntry.setFrequencySize((int) (numPostings*4));

                    vocOffset = vocEntry.writeEntryToDisk(vocOffset, vocabularyFchan);
                    if(debugMode){
                        list.debugSaveToDisk("partialDOCIDS_"+numIndex+".txt", "partialFREQS_"+numIndex+".txt", (int) numPostings);
                        vocEntry.debugSaveToDisk("partialVOC_"+numIndex+".txt");
                    }
                }
            }

            //update number of partial inverted indexes and vocabularies
            numIndex++;
            numPostings = 0;
            return true;
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
            return false;
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
            return false;
        }
    }


    /**
     * Function that searched for a given docid in a posting list.
     * If the document is already present it updates the term frequency for that
     * specific document, if that's not the case creates a new pair (docid,freq)
     * in which frequency is set to 1 and adds this pair to the posting list
     *
     * @param docid:       docid of a certain document
     * @param postingList: posting list of a given term
     **/
    protected static void updateOrAddPosting(int docid, PostingList postingList) {
        if (postingList.getPostings().size() > 0) {
            // last document inserted:
            Posting posting = postingList.getPostings().get(postingList.getPostings().size() - 1);
            //If the docId is the same I update the posting
            if (docid == posting.getDocid()) {
                posting.setFrequency(posting.getFrequency() + 1);
                return;
            }
        }
        // the document has not been processed (docIds are incremental):
        // create new pair and add it to the posting list
        postingList.getPostings().add(new Posting(docid, 1));

        //increment the number of postings
        numPostings++;

    }

    /**
     * Performs spimi algorithm
     *
     * @return the number of partial indexes created
     * @param compressedReadingEnable flag enabling reading from compressed file and stemming if true
     * @param debug flag enabling debug mode
     */
    public static int executeSpimi(boolean compressedReadingEnable,boolean debug) {

        try (
            BufferedReader br = initBuffer(compressedReadingEnable)
        ) {
            boolean allDocumentsProcessed = false; //is set to true when all documents are read

            int docid = 1; //assign docid in a incremental manner
            int docsLen = 0; // total sum of lengths of documents
            boolean writeSuccess; //checks whether the writing of the partial data structures was successful or not

            long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
            String[] split;
            while (!allDocumentsProcessed ) {
                HashMap<String, PostingList> index = new HashMap<>(); //hashmap containing partial index
                while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD ) { //build index until 80% of total memory is used

                    String line;
                    // if we reach the end of file (br.readline() -> null)
                    if ((line = br.readLine()) == null) {
                        // we've processed all the documents
                        allDocumentsProcessed = true;
                        break;
                    }
                    // if the line is empty we process the next line
                    if (line.isBlank())
                        continue;

                    // split of the line in the format <pid>\t<text>
                    split = line.split("\t");

                    // Creation of the text document for the line
                    TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));
                    // Perform text preprocessing on the document

                    ProcessedDocument processedDocument = Preprocesser.processDocument(document);

                    if (processedDocument.getTokens().isEmpty())
                        continue;


                    int documentLength = processedDocument.getTokens().size();
                    //create new document index entry and add it to file
                    DocumentIndexEntry docIndexEntry = new DocumentIndexEntry(
                            processedDocument.getPid(),
                            docid,
                            documentLength
                    );

                    //update with length of new documents
                    docsLen += docIndexEntry.getDocLen();

                    // write the docIndex entry to disk
                    docIndexEntry.writeToDisk();

                    if(debug){
                        docIndexEntry.debugWriteToDisk("debugDOCINDEX.txt");
                    }

                    for (String term : processedDocument.getTokens()) {

                        if(term.isBlank())
                            continue;

                        PostingList posting; //posting list of a given term
                        if (!index.containsKey(term)) {
                            // create new posting list if term wasn't present yet
                            posting = new PostingList(term);
                            index.put(term, posting); //add new entry (term, posting list) to entry
                        } else {
                            //term is present, we can get its posting list
                            posting = index.get(term);
                        }

                        //insert or update new posting
                        updateOrAddPosting(docid, posting);
                        posting.updateBM25Parameters(documentLength, posting.getPostings().size());

                    }
                    docid++;
                    if((docid%1000000)==0){
                        System.out.println("at docid: "+docid);
                    }
                }

                //either if there is no  memory available or all documents were read, flush partial index onto disk
                writeSuccess = saveIndexToDisk(index, debug);

                //error during data structures creation. Rollback previous operations and end algorithm
                if(!writeSuccess){
                    System.out.println("Couldn't write index to disk.");
                    rollback();
                    return -1;
                }
                index.clear();

            }
            // update the size of the document index and save it to disk
            if(!CollectionSize.updateCollectionSize(docid) || !CollectionSize.updateDocumentsLenght(docsLen)){
                System.out.println("Couldn't update collection statistics.");
                return 0;
            }


            return numIndex;

        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }


    }




}