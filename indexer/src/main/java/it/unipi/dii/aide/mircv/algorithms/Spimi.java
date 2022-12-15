package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.CharBuffer;
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


    //TODO: transform in HashMap
    /*
    counts the number of partial indexes created
     */
    private static int numIndex = 0;

    private static int numPostings = 0;


    //TODO: error handling

    /**
     * @param index:   partial index that must be saved onto file
     * @param numDocs: number of documents processed
     */
    private static void saveIndexToDisk(HashMap<String, PostingList> index, int numDocs) {

        if (index.isEmpty()) //if the index is empty there is nothing to write on disk
            return;

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

            MappedByteBuffer vocBuffer = vocabularyFchan.map(FileChannel.MapMode.READ_WRITE, 0, (long) numPostings * VocabularyEntry.getENTRY_SIZE());
            // check if MappedByteBuffers are correctly instantiated
            if (docsBuffer != null && freqsBuffer != null && vocBuffer != null) {
                for (PostingList list : index.values()) {
                    // write postings to file
                    for (Map.Entry<Integer, Integer> posting : list.getPostings()) {
                        // encode docid
                        docsBuffer.putInt(posting.getKey());
                        // encode freq
                        freqsBuffer.putInt(posting.getValue());
                    }
                    //create vocabulary entry
                    VocabularyEntry entry = new VocabularyEntry(list.getTerm());

                    //compute entry statistics
                    entry.updateStatistics(list);

                    //set size of memory offset
                    entry.setMemoryOffset(docsBuffer.position());

                    //set size of frequency offset
                    entry.setFrequencyOffset(freqsBuffer.position());

                    //set size of posting list
                    entry.setMemorySize(list.getNumBytes());

                    //allocate char buffer to write term
                    CharBuffer charBuffer = CharBuffer.allocate(VocabularyEntry.TERM_SIZE);

                    String term = entry.getTerm();

                    //TODO: fix after preprocessing is fixed
                    //populate char buffer char by char
                    int len = term.length();
                    if(term.length() > VocabularyEntry.TERM_SIZE)
                        len = VocabularyEntry.TERM_SIZE;
                    for (int i = 0; i < len; i++)
                        charBuffer.put(i, term.charAt(i));

                    // Write the term into file
                    vocBuffer.put(StandardCharsets.UTF_8.encode(charBuffer));

                    // Write the document frequency into file
                    vocBuffer.putInt(entry.getDf());

                    // Write the term frequency into file
                    vocBuffer.putInt(entry.getTf());

                    //wirte IDF into file
                    vocBuffer.putDouble(entry.getIdf());

                    //write memory offset into file
                    vocBuffer.putLong(entry.getMemoryOffset());

                    //write frequency offset into file
                    vocBuffer.putLong(entry.getFrequencyOffset());

                    //write memory offset into file
                    vocBuffer.putLong(entry.getMemorySize());
                }
            }
            //update number of partial inverted indexes and vocabularies
            numIndex++;
            numPostings = 0;
        } catch (InvalidPathException e) {
            System.out.println("Path Error " + e);
        } catch (IOException e) {
            System.out.println("I/O Error " + e);
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
    private static void updateOrAddPosting(int docid, PostingList postingList) {
        if (postingList.getPostings().size() > 0) {
            // last document inserted:
            Map.Entry<Integer, Integer> posting = postingList.getPostings().get(postingList.getPostings().size() - 1);
            //If the docId is the same I update the posting
            if (docid == posting.getKey()) {
                posting.setValue(posting.getValue() + 1);
                return;
            }
        }
        // the document has not been processed (docIds are incremental):
        // create new pair and add it to the posting list
        postingList.getPostings().add(new AbstractMap.SimpleEntry<>(docid, 1));

        //increment tne number of postings
        numPostings++;

    }

    /**
     * Performs spimi algorithm
     *
     * @return the number of partial indexes created
     */
    public static int executeSpimi() {

        //create directories to store partial frequencies, docids and vocabularies
        FileUtils.createDirectory(ConfigurationParameters.getDocidsDir());
        FileUtils.createDirectory(ConfigurationParameters.getFrequencyDir());
        FileUtils.createDirectory(ConfigurationParameters.getPartialVocabularyDir());

        try (
                BufferedReader br = Files.newBufferedReader(Paths.get(PATH_TO_COLLECTION), StandardCharsets.UTF_8);

        ) {
            boolean allDocumentsProcessed = false; //is set to true when all documents are read

            int docid = 0; //assign docid in a incremental manner
            int partialNumDocs = 0;

            long MEMORY_THRESHOLD = Runtime.getRuntime().totalMemory() * 20 / 100; // leave 20% of memory free
            String[] split;
            while (!allDocumentsProcessed) {
                HashMap<String, PostingList> index = new HashMap<>(); //hashmap containing partial index
                while (Runtime.getRuntime().freeMemory() > MEMORY_THRESHOLD) { //build index until 80% of total memory is used

                    String line;
                    // if we reach the end of file (br.readline() -> null)
                    if ((line = br.readLine()) == null) {
                        // we've processed all the documents
                        allDocumentsProcessed = true;
                        break;
                    }
                    // if the line is empty we process the next line
                    if (line.isEmpty())
                        continue;

                    // split of the line in the format <pid>\t<text>
                    split = line.split("\t");

                    // Creation of the text document for the line
                    TextDocument document = new TextDocument(split[0], split[1].replaceAll("[^\\x00-\\x7F]", ""));
                    // Perform text preprocessing on the document
                    ProcessedDocument processedDocument = Preprocesser.processDocument(document);

                    if (processedDocument.getTokens().isEmpty()) {
                        continue;
                    }

                    //create new document index entry and add it to file
                    DocumentIndexEntry entry = new DocumentIndexEntry(
                            processedDocument.getPid(),
                            docid++,
                            processedDocument.getTokens().size()
                    );


                    // write the docIndex entry to disk
                    entry.writeToDisk();
                    partialNumDocs++;

                    for (String term : processedDocument.getTokens()) {

                        if (term.length() == 0)
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

                    }
                    System.out.println(docid);
                }
                if (partialNumDocs > 0)
                    //either if there is no  memory available or all documents were read, flush partial index onto disk
                    saveIndexToDisk(index, partialNumDocs);
                partialNumDocs = 0;
                index.clear();
            }
            // update the size of the document index and save it to disk
            if (!CollectionSize.updateCollectionSize(docid))
                return 0;
            return numIndex;

        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }


}
