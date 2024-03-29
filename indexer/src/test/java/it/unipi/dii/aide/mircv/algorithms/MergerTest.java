package it.unipi.dii.aide.mircv.algorithms;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.Flags;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.junit.jupiter.api.*;

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


    private static final String TEST_DIRECTORY = "src/test/data";
    private static final String PATH_TO_PARTIAL_VOCABULARY = TEST_DIRECTORY + "/partial_vocabulary/partial_vocabulary";
    private static final String PATH_TO_PARTIAL_FREQUENCIES = TEST_DIRECTORY+"/partial_freqs/partial_freqs";
    private static final String PATH_TO_PARTIAL_INDEXES_DOCS = TEST_DIRECTORY+"/partial_docids/partial_docids";
    private static final String DOCINDEX_PATH = TEST_DIRECTORY+"/docIndex";
    private static final String VOCABULARY_PATH = TEST_DIRECTORY+"/vocabulary";
    private static final String INVERTED_INDEX_DOCIDS = TEST_DIRECTORY+"/docids";
    private static final String INVERTED_INDEX_FREQS = TEST_DIRECTORY+"/freqs";
    private static final String BLOCK_DESCRIPTOR_PATH = TEST_DIRECTORY + "/block_descriptors";
    private static final String COLLECTION_STATISTICS_PATH = TEST_DIRECTORY + "/collection_statistics";

    @BeforeAll
    static void setPaths(){
        FileUtils.deleteDirectory(TEST_DIRECTORY);
        //FileUtils.createDirectory(TEST_DIRECTORY);
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
        FileUtils.createDirectory(TEST_DIRECTORY);
        createDirectory(TEST_DIRECTORY+"/partial_freqs");
        createDirectory(TEST_DIRECTORY+"/partial_docids");
        createDirectory(TEST_DIRECTORY+"/partial_vocabulary");
        BlockDescriptor.setMemoryOffset(0);
        Vocabulary.unsetInstance();
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

        CollectionSize.updateCollectionSize(docIndex.size());
        CollectionSize.setTotalDocLen(22);
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

        // merging intermediate indexes
        assertTrue(Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, false), "Error: merging failed");

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

    private void mergeTwoIndexes(boolean compressionMode, boolean vocabularyTest) {
        // building partial index 1
        ArrayList<PostingList> postings = new ArrayList<>();

        PostingList pl = new PostingList("amburgo\t1:3 2:2: 3:5");
        pl.updateBM25Parameters(1,3);
        postings.add(pl);
        pl = new PostingList("pisa\t2:1 3:2");
        pl.updateBM25Parameters(4,1);
        postings.add(pl);
        pl = new PostingList("zurigo\t2:1 3:2");
        pl.updateBM25Parameters(4,1);
        postings.add(pl);

        // building partial index 2
        ArrayList<PostingList> index2 = new ArrayList<>();
        pl = new PostingList("alberobello\t4:3 5:1");
        pl.updateBM25Parameters(1,3);
        index2.add(pl);
        pl = new PostingList("pisa\t5:2");
        pl.updateBM25Parameters(3, 2);
        index2.add(pl);

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

        // merging intermediate indexes
        assertTrue(Merger.mergeIndexes(intermediateIndexes.size(), compressionMode, false), "Error: merging failed");

        if(vocabularyTest){
            ArrayList<VocabularyEntry> expectedVocabulary = new ArrayList<>();
            VocabularyEntry vocEntry = new VocabularyEntry("alberobello");
            if(compressionMode){
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(0);
                vocEntry.setFrequencyOffset(0);
            } else {
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(0);
                vocEntry.setFrequencyOffset(0);
            }
            vocEntry.setNumBlocks(1);
            vocEntry.setBlockOffset(0);
            vocEntry.setDf(2);
            vocEntry.setIdf(0.3979400086720376);
            vocEntry.setMaxTf(3);
            vocEntry.setMaxTFIDF(0.5878056449127935);
            vocEntry.setBM25Tf(3);
            vocEntry.setBM25Dl(1);
            vocEntry.setMaxBM25(0.3288142794660968);

            expectedVocabulary.add(vocEntry);


            vocEntry = new VocabularyEntry("amburgo");
            if(compressionMode){
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(2);
                vocEntry.setFrequencyOffset(1);
            } else {
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(8);
                vocEntry.setFrequencyOffset(8);
            }
            vocEntry.setNumBlocks(1);
            vocEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES);
            vocEntry.setDf(3);
            vocEntry.setIdf(0.22184874961635637);
            vocEntry.setMaxTf(5);
            vocEntry.setMaxTFIDF(0.3769143710976413);
            vocEntry.setBM25Tf(3);
            vocEntry.setBM25Dl(1);
            vocEntry.setMaxBM25(0.18331164287548693);

            expectedVocabulary.add(vocEntry);


            vocEntry = new VocabularyEntry("pisa");
            if(compressionMode){
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(5);
                vocEntry.setFrequencyOffset(3);
            } else {
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(20);
                vocEntry.setFrequencyOffset(20);
            }
            vocEntry.setNumBlocks(1);
            vocEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES*2);
            vocEntry.setDf(3);
            vocEntry.setIdf(0.22184874961635637);
            vocEntry.setMaxTf(2);
            vocEntry.setMaxTFIDF(0.2886318777514278);
            vocEntry.setBM25Tf(2);
            vocEntry.setBM25Dl(3);
            vocEntry.setMaxBM25(0.1412129473145704);

            expectedVocabulary.add(vocEntry);


            vocEntry = new VocabularyEntry("zurigo");
            if(compressionMode){
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(8);
                vocEntry.setFrequencyOffset(4);
            } else {
                vocEntry.setDocidSize(0);
                vocEntry.setFrequencySize(0);
                vocEntry.setMemoryOffset(32);
                vocEntry.setFrequencyOffset(32);
            }

            vocEntry.setNumBlocks(1);
            vocEntry.setBlockOffset(BlockDescriptor.BLOCK_DESCRIPTOR_ENTRY_BYTES*3);
            vocEntry.setDf(2);
            vocEntry.setIdf(0.3979400086720376);
            vocEntry.setMaxTf(2);
            vocEntry.setMaxTFIDF(0.5177318877571058);
            vocEntry.setBM25Tf(1);
            vocEntry.setBM25Dl(4);
            vocEntry.setMaxBM25(0.16596550124710574);

            expectedVocabulary.add(vocEntry);

            // read vocabulary from disk
            Vocabulary v = Vocabulary.getInstance();
            v.readFromDisk();

            ArrayList<VocabularyEntry> retrievedVocabulary = new ArrayList<>();
            retrievedVocabulary.addAll(v.values());

            assertArrayEquals(expectedVocabulary.toArray(), retrievedVocabulary.toArray(), "Vocabulary after merging is different from the expected vocabulary.");

        } else {

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



    }


    /* test merging of a single index without compression
     *      index tested:
     *          - "alberobello" = {(1,3), (2,3), (4,7)}
     *          - "newyork" = {(1,5), (3,2), (4,6)}
     *          - "pisa" = {(1,1), (5,3)}
     */
    @Test
    void singleIndexMergeWithoutCompression(){
        Flags.setCompression(false);
        mergeSingleIndex(false);
    }

    /* test merging of a single index with compression
     *      index tested:
     *          - "alberobello" = {(1,3), (2,3), (4,7)}
     *          - "newyork" = {(1,5), (3,2), (4,6)}
     *          - "pisa" = {(1,1), (5,3)}
     */
    @Test
    void singleIndexMergeWithCompression() {
        Flags.setCompression(true);
        mergeSingleIndex(true);
    }


    /* test merging of two indexes without compression
     *      index 1:
     *          - "amburgo" = {(1,3), (2,2), (3,5)}
     *          - "pisa" = {(2,1), (3,2)}
     *          - "zurigo" = {(4,1)}
     *      index 2:
     *          - "alberobello" = {(4,3), (5,1)}
     *          - "pisa" = {(5,2)}
     */
    @Test
    void twoIndexesMergeWithoutCompression() {
        Flags.setCompression(false);
        mergeTwoIndexes(false, false);
    }

    /* test merging of two indexes with compression
     *      index 1:
     *          - "amburgo" = {(1,3), (2,2), (3,5)}
     *          - "pisa" = {(2,1), (3,2)}
     *          - "zurigo" = {(4,1)}
     *      index 2:
     *          - "alberobello" = {(4,3), (5,1)}
     *          - "pisa" = {(5,2)}
     */
    @Test
    void twoIndexesMergeWithCompression() {
        Flags.setCompression(true);
        mergeTwoIndexes(true, false);
    }

    /* test vocabulary after merging of two indexes without compression
     *      index 1:
     *          - "amburgo" = {(1,3), (2,2), (3,5)}
     *          - "pisa" = {(2,1), (3,2)}
     *          - "zurigo" = {(4,1)}
     *      index 2:
     *          - "alberobello" = {(4,3), (5,1)}
     *          - "pisa" = {(5,2)}
     */
    @Test
    void vocabularyTest(){
        Flags.setCompression(false);
        mergeTwoIndexes(false, true);
    }

    /* test vocabulary after merging of two indexes without compression
     *      index 1:
     *          - "amburgo" = {(1,3), (2,2), (3,5)}
     *          - "pisa" = {(2,1), (3,2)}
     *          - "zurigo" = {(4,1)}
     *      index 2:
     *          - "alberobello" = {(4,3), (5,1)}
     *          - "pisa" = {(5,2)}
     */
    @Test
    void vocabularyTest2() {
        Flags.setCompression(true);
        mergeTwoIndexes(true, true);
    }

    @AfterAll
    static void teardown() {
        FileUtils.deleteDirectory(TEST_DIRECTORY + "/partial_docids");
        FileUtils.deleteDirectory(TEST_DIRECTORY + "/partial_freqs");
        FileUtils.deleteDirectory(TEST_DIRECTORY + "/partial_vocabulary");
    }


}