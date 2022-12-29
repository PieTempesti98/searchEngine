package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.Flags;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.createDirectory;
import static org.junit.jupiter.api.Assertions.*;

class MergerTest {


    private static final String TEST_DIRECTORY = "data/test";
    private static final String PATH_TO_PARTIAL_VOCABULARY = TEST_DIRECTORY+"/partial_vocabulary/partial_vocabulary";
    private static final String PATH_TO_PARTIAL_FREQUENCIES = TEST_DIRECTORY+"/partial_freqs/partial_freqs";
    private static final String PATH_TO_PARTIAL_INDEXES_DOCS = TEST_DIRECTORY+"/partial_docids/partial_docids";
    private static final String DOCINDEX_PATH = TEST_DIRECTORY+"/docIndex";
    private static final String VOCABULARY_PATH = TEST_DIRECTORY+"/vocabulary";
    private static final String INVERTED_INDEX_DOCIDS = TEST_DIRECTORY+"/docids";
    private static final String INVERTED_INDEX_FREQS = TEST_DIRECTORY+"/freqs";
    private static final String BLOCK_DESCRIPTOR_PATH = TEST_DIRECTORY+"/block_descriptors";
    private static final String COLLECTION_STATISTICS_PATH = TEST_DIRECTORY+"collection_statistics";

    @BeforeAll
    static void setPaths(){
        FileUtils.deleteDirectory(TEST_DIRECTORY);
        FileUtils.createDirectory(TEST_DIRECTORY);
        Merger.setPathToVocabulary(VOCABULARY_PATH);
        Merger.setPathToInvertedIndexDocs(INVERTED_INDEX_DOCIDS);
        Merger.setPathToInvertedIndexFreqs(INVERTED_INDEX_FREQS);
        Merger.setPathToBlockDescriptors(BLOCK_DESCRIPTOR_PATH);
        Merger.setPathToPartialIndexesDocs(PATH_TO_PARTIAL_INDEXES_DOCS);
        Merger.setPathToPartialIndexesFreqs(PATH_TO_PARTIAL_FREQUENCIES);
        Merger.setPathToPartialVocabularies(PATH_TO_PARTIAL_VOCABULARY);
        VocabularyEntry.setBlockDescriptorsPath(BLOCK_DESCRIPTOR_PATH);
        BlockDescriptor.setInvertedIndexDocs(INVERTED_INDEX_DOCIDS);
        BlockDescriptor.setInvertedIndexFreqs(INVERTED_INDEX_FREQS);
        CollectionSize.setCollectionStatisticsPath(COLLECTION_STATISTICS_PATH);
        Vocabulary.setVocabularyPath(VOCABULARY_PATH);
        if(Flags.isStemStopRemovalEnabled())
            Preprocesser.readStopwords();
    }


    @BeforeEach
    void setUp() {
        //create directories to store partial frequencies, docids and vocabularies
        createDirectory(TEST_DIRECTORY+"/partial_freqs");
        createDirectory(TEST_DIRECTORY+"/partial_docids");
        createDirectory(TEST_DIRECTORY+"/partial_vocabulary");
    }

    @AfterEach
    void tearDown() {
        //delete directories to store partial frequencies, docids and vocabularies
        FileUtils.deleteDirectory(TEST_DIRECTORY);
    }
    private static boolean writeIntermediateIndexesToDisk(ArrayList<ArrayList<PostingList>> intermediateIndexes) {
        for (ArrayList<PostingList> intermediateIndex : intermediateIndexes) {

            int i = intermediateIndexes.indexOf(intermediateIndex);

            try (
                    FileChannel docsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_INDEXES_DOCS + "_"+i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE
                    );
                    FileChannel freqsFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_FREQUENCIES +"_"+ i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE);
                    FileChannel vocabularyFchan = (FileChannel) Files.newByteChannel(Paths.get(PATH_TO_PARTIAL_VOCABULARY +"_"+ i),
                            StandardOpenOption.WRITE,
                            StandardOpenOption.READ,
                            StandardOpenOption.CREATE)
            ) {
                long vocOffset = 0;
                long docidOffset = 0;
                long freqOffset = 0;
                for (PostingList postingList : intermediateIndex) {

                    int numPostings = intermediateIndex.size();
                    // instantiation of MappedByteBuffer for integer list of docids and for integer list of freqs
                    MappedByteBuffer docsBuffer = docsFchan.map(FileChannel.MapMode.READ_WRITE, docidOffset, numPostings * 4L);
                    MappedByteBuffer freqsBuffer = freqsFchan.map(FileChannel.MapMode.READ_WRITE, freqOffset, numPostings * 4L);

                    // check if MappedByteBuffers are correctly instantiated
                    if (docsBuffer != null && freqsBuffer != null) {
                        //create vocabulary entry
                        VocabularyEntry vocEntry = new VocabularyEntry(postingList.getTerm());
                        vocEntry.setMemoryOffset(docsBuffer.position());
                        vocEntry.setFrequencyOffset(docsBuffer.position());

                        // write postings to file
                        for (Posting posting : postingList.getPostings()) {
                            // encode docid and freq
                            docsBuffer.putInt(posting.getDocid());
                            freqsBuffer.putInt(posting.getFrequency());

                        }
                        vocEntry.updateStatistics(postingList);
                        vocEntry.setBM25Dl(postingList.getBM25Dl());
                        vocEntry.setBM25Tf(postingList.getBM25Tf());
                        vocEntry.setDocidSize(numPostings*4);
                        vocEntry.setFrequencySize(numPostings*4);

                        vocEntry.setMemoryOffset(docidOffset);
                        vocEntry.setFrequencyOffset(freqOffset);

                        //System.out.println("saving vocentry "+vocEntry);

                        vocOffset = vocEntry.writeEntryToDisk(vocOffset, vocabularyFchan);

                        docidOffset += numPostings * 4L;
                        freqOffset += numPostings * 4L;

                    } else {
                        return false;
                    }
                }
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private static ArrayList<ArrayList<Posting>> retrieveIndexFromDisk(){
        // get vocabulary from disk
        Vocabulary v = Vocabulary.getInstance();
        v.readFromDisk();

        ArrayList<ArrayList<Posting>> mergedLists = new ArrayList<>(v.size());

        ArrayList<VocabularyEntry> vocEntries = new ArrayList<>();
        vocEntries.addAll(v.values());

        for(VocabularyEntry vocabularyEntry: vocEntries){
            //System.out.println("retrieving "+vocabularyEntry);
            PostingList p = new PostingList();
            p.setTerm(vocabularyEntry.getTerm());
            p.openList();
            ArrayList<Posting> postings = new ArrayList<>();


            while(p.next()!=null){
                postings.add(p.getCurrentPosting());
            }

            p.closeList();

            mergedLists.add(postings);
        }
        return mergedLists;
    }



    private static LinkedHashMap<Integer, DocumentIndexEntry> buildDocIndex(ArrayList<ArrayList<PostingList>> indexes){
        LinkedHashMap<Integer, DocumentIndexEntry> docIndex = new LinkedHashMap<>();
        int docCounter = 0;

        for(ArrayList<PostingList> index: indexes){
            for(PostingList postingList: index){
                for(Posting posting: postingList.getPostings()){
                    DocumentIndexEntry docEntry = docIndex.get(posting.getDocid());
                    if(docEntry!=null){
                        docEntry.setDocLen(docEntry.getDocLen()+posting.getFrequency());
                    } else {
                        docEntry = new DocumentIndexEntry(Integer.toString(posting.getDocid()), docCounter, posting.getFrequency());
                        docIndex.put(posting.getDocid(), docEntry);
                        docCounter++;
                    }
                }
            }

        }
        return docIndex;
    }

    public static boolean writeDocumentIndexToDisk(LinkedHashMap<Integer, DocumentIndexEntry> docIndex) {

        // try to open a file channel to the file of the inverted index
        try (FileChannel fChan = (FileChannel) Files.newByteChannel(
                Paths.get(DOCINDEX_PATH),
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE))
        {
            int memOffset = 0;
            for(DocumentIndexEntry documentIndexEntry: docIndex.values()){
                // instantiation of MappedByteBuffer for the entry
                MappedByteBuffer buffer = fChan.map(FileChannel.MapMode.READ_WRITE, memOffset, DocumentIndexEntry.ENTRY_SIZE);

                // Buffer not created
                if(buffer == null)
                    return false;

                // Create the CharBuffer with size = PID_SIZE
                CharBuffer charBuffer = CharBuffer.allocate(DocumentIndexEntry.PID_SIZE);
                for(int i = 0; i < documentIndexEntry.getPid().length(); i++)
                    charBuffer.put(i, documentIndexEntry.getPid().charAt(i));

                // Write the PID into file
                buffer.put(StandardCharsets.UTF_8.encode(charBuffer));

                // Write the docid into file
                buffer.putInt(documentIndexEntry.getDocid());
                // Write the doclen into file
                buffer.putInt(documentIndexEntry.getDocLen());

                // update memory offset
                memOffset += DocumentIndexEntry.ENTRY_SIZE;
            }

        } catch(Exception e){
            e.printStackTrace();
            return false;
        }

        //CollectionSize.updateCollectionSize(docIndex.size());
        return true;
    }


    public void mergeSingleIndex(Boolean compressionMode){

        // building partial index 1
        ArrayList<PostingList> index1 = new ArrayList<>();

        index1.add(new PostingList("alberobello\t1:3 2:3: 4:7"));
        index1.add(new PostingList("newyork\t1:5 3:2: 4:6"));
        index1.add(new PostingList("pisa\t1:1 5:3"));

        // insert partial index to array of partial indexes
        ArrayList<ArrayList<PostingList>> intermediateIndexes = new ArrayList<>();
        intermediateIndexes.add(index1);

        // build document index for intermediate indexes
        LinkedHashMap<Integer, DocumentIndexEntry> docIndex = buildDocIndex(intermediateIndexes);

        // write document index to disk
        assertTrue(writeDocumentIndexToDisk(docIndex), "Error while writing document index to disk");

        // write intermediate indexes to disk so that SPIMI can be executed
        assertTrue(writeIntermediateIndexesToDisk(intermediateIndexes), "Error while writing intermediate indexes to disk");

        //System.out.println("start merging");

        // merging intermediate indexes
        assertTrue(Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, true), "Error: merging failed");

        //System.out.println("merged");

        ArrayList<ArrayList<Posting>> mergedLists = retrieveIndexFromDisk();

        assertNotNull(mergedLists, "Error, merged index is empty");

        // build expected results
        ArrayList<ArrayList<Posting>> expectedResults = new ArrayList<>(3);

        ArrayList<Posting> postings = new ArrayList<>();
        postings.addAll(List.of(new Posting[]{
                new Posting(1, 3),
                new Posting(2, 3),
                new Posting(4,7)
        }));
        expectedResults.add(postings);
        postings = new ArrayList<>();
        postings.addAll(List.of(new Posting[]{
                new Posting(1,5),
                new Posting(3,2),
                new Posting(4,6)
        }));
        expectedResults.add(postings);

        postings = new ArrayList<>();
        postings.addAll(List.of(new Posting[]{
                new Posting(1,1),
                new Posting(5,3)
        }));
        expectedResults.add(postings);

        assertEquals(expectedResults.toString(), mergedLists.toString(), "Error, expected results are different from actual results.");
    }

    private void mergeTwoIndexes(boolean compressionMode) {
        // building partial index 1
        ArrayList<PostingList> postings = new ArrayList<>();

        postings.add(new PostingList("amburgo\t1:3 2:2: 3:5"));
        postings.add(new PostingList("pisa\t2:1 3:2"));
        postings.add(new PostingList("zurigo\t2:1 3:2"));

        // building partial index 2
        ArrayList<PostingList> index2 = new ArrayList<>();

        index2.add(new PostingList("alberobello\t4:3 5:1"));
        index2.add(new PostingList("pisa\t5:2"));

        // insert partial index to array of partial indexes
        ArrayList<ArrayList<PostingList>> intermediateIndexes = new ArrayList<>();
        intermediateIndexes.add(postings);
        intermediateIndexes.add(index2);

        // build document index for intermediate indexes
        LinkedHashMap<Integer, DocumentIndexEntry> docIndex = buildDocIndex(intermediateIndexes);

        // write document index to disk
        assertTrue(writeDocumentIndexToDisk(docIndex), "Error while writing document index to disk");

        // write intermediate indexes to disk
        assertTrue(writeIntermediateIndexesToDisk(intermediateIndexes), "Error while writing intermediate indexes to disk");

        //System.out.println("start merging");

        // merging intermediate indexes
        assertTrue(Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, true), "Error: merging failed");

        //System.out.println("merged");

        ArrayList<ArrayList<Posting>> mergedLists = retrieveIndexFromDisk();


        // build expected results
        ArrayList<ArrayList<Posting>> expectedResults = new ArrayList<>(4);

        ArrayList<Posting> expectedPostings = new ArrayList<>();
        expectedPostings.addAll(List.of(new Posting[]{
                new Posting(4,3),
                new Posting(5,1),
        }));
        expectedResults.add(expectedPostings);
        expectedPostings = new ArrayList<>();

        expectedPostings.addAll(List.of(new Posting[]{
                new Posting(1, 3),
                new Posting(2, 2),
                new Posting(3,5)
        }));
        expectedResults.add(expectedPostings);

        expectedPostings = new ArrayList<>();
        expectedPostings.addAll(List.of(new Posting[]{
                new Posting(2,1),
                new Posting(3,2),
                new Posting(5,2)
        }));
        expectedResults.add(expectedPostings);
        expectedPostings = new ArrayList<>();
        expectedPostings.addAll(List.of(new Posting[]{
                new Posting(2,1),
                new Posting(3,2),
        }));
        expectedResults.add(expectedPostings);

        assertEquals(expectedResults.toString(), mergedLists.toString(), "Error, expected results are different from actual results.");
    }

    private void testVocabulary(boolean compressed) {
        // TODO: implement
        

    }


    @Test
    void singleIndexMergeWithoutCompression(){
        Flags.setCompression(false);
        mergeSingleIndex(false);
    }


    @Test
    void singleIndexMergeWithCompression() {
        Flags.setCompression(true);
        mergeSingleIndex(true);
    }


    @Test
    void twoIndexesMergeWithoutCompression() {
        Flags.setCompression(false);
        mergeTwoIndexes(false);
    }

    @Test
    void twoIndexesMergeWithCompression() {
        Flags.setCompression(true);
        mergeTwoIndexes(true);
    }

    @Test
    void vocabularyTest(){
        Flags.setCompression(false);
        testVocabulary(false);
    }




}