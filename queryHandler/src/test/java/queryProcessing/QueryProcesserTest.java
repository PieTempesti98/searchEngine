package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.Flags;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;


class QueryProcesserTest {
    private static final String VOCABULARY_PATH = "src/test/data/vocabulary";
    private static final String BLOCK_DESCRIPTORS_PATH = "src/test/data/blockDescriptors";
    private static final String DOCINDEX_PATH = "src/test/data/documentIndex";
    private static final String INVERTED_INDEX_DOCS = "src/test/data/invertedIndexDocs";
    private static final String INVERTED_INDEX_FREQS = "src/test/data/invertedIndexFreqs";

    private static final DocumentIndex docIndex = DocumentIndex.getInstance();

    @BeforeAll
    public static void init() {
        CollectionSize.setTotalDocLen(61);
        CollectionSize.setCollectionSize(8);

        Vocabulary.setVocabularyPath(VOCABULARY_PATH);
        VocabularyEntry.setBlockDescriptorsPath(BLOCK_DESCRIPTORS_PATH);
        BlockDescriptor.setInvertedIndexDocs(INVERTED_INDEX_DOCS);
        BlockDescriptor.setInvertedIndexFreqs(INVERTED_INDEX_FREQS);

        DocumentIndexEntry.setDocindexPath(DOCINDEX_PATH);
        boolean success = docIndex.loadFromDisk();
        assertTrue(success);

    }

    @BeforeEach
    public void initVocab(){
        Vocabulary.unsetInstance();
        Vocabulary v = Vocabulary.getInstance();
        boolean success = v.readFromDisk();
        assertTrue(success);
    }

    public static Stream<Arguments> getBM25Parameters() {
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleConjBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleDisBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleDisBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleConjBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsEmpty = new PriorityQueue<>(3, Map.Entry.comparingByKey());

        //queue for query "another example" conjunctive mode with bm25
        expectedResultsAnotherExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.2582940702253402, 8));
        expectedResultsAnotherExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.38158664142011345, 2));


        //queue for query "another example" disjunctive mode with bm25
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.1123005090598549, 3));
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.38158664142011345, 2));
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.2582940702253402, 8));

        //queue for query "example" disjunctive mode with bm25
        expectedResultsExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.09030875025937561, 5));
        expectedResultsExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.1123005090598549, 3));
        expectedResultsExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.09661547190697509, 2));

        //queue for query "example" conjunctive mode with bm25
        expectedResultsExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.09030875025937561, 5));
        expectedResultsExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.1123005090598549, 3));
        expectedResultsExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.09661547190697509, 2));

        //postings for query "another example"
        ArrayList<PostingList> queryPostingsAnotherExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example"), new PostingList("another")}).toList());

        //postings for query "example"
        ArrayList<PostingList> queryPostingsExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example")}).toList());

        //postings for query "simple example"
        ArrayList<PostingList> queryPostingsSimpleExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example"), new PostingList("simple")}).toList());

        return Stream.of(Arguments.arguments(3, queryPostingsAnotherExample, true, expectedResultsAnotherExampleConjBM25),
                Arguments.arguments(3, queryPostingsAnotherExample, false, expectedResultsAnotherExampleDisBM25),
                Arguments.arguments(3, queryPostingsExample, false, expectedResultsExampleDisBM25),
                Arguments.arguments(3, queryPostingsExample, true, expectedResultsExampleConjBM25),
                Arguments.arguments(3, queryPostingsSimpleExample, true, expectedResultsEmpty)
        );
    }

    public Object[] reformatQueue(PriorityQueue<Map.Entry<Double, Integer>> queue) {

        //arraylist storing the result
        ArrayList<AbstractMap.SimpleEntry<Double, Integer>> returnList = new ArrayList<>();

        //get array from queue
        Object[] queueArray = queue.toArray();

        //populate array list
        for (int i = 0; i < queueArray.length; i++)
            returnList.add((AbstractMap.SimpleEntry<Double, Integer>) queueArray[i]);

        //sort arraylist since there is no guarantee of order of priority queue after making it an array
        returnList.sort(Map.Entry.comparingByKey());

        //cast to array
        return returnList.toArray();


    }

    public static Stream<Arguments> getTFIDFParameters() {
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleConjTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleDisTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleDisTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleConjTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsEmpty = new PriorityQueue<>(3, Map.Entry.comparingByKey());

        //queue for query "another example" conjunctive mode with tfidf
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.8061799739838872, 2));
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.9874180905628003, 8));

        //queue for query "another example" disjunctive mode with tfidf
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.8061799739838872, 2));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 6));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.9874180905628003, 8));

        //queue for query "example" disjunctive mode with tfidf
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 5));
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 3));
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 6));

        //queue for query "example" conjunctive mode with tfidf
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 5));
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 3));
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 6));

        //postings for query "another example"
        ArrayList<PostingList> queryPostingsAnotherExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example"), new PostingList("another")}).toList());

        //postings for query "example"
        ArrayList<PostingList> queryPostingsExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example")}).toList());

        //postings for query "simple example"
        ArrayList<PostingList> queryPostingsSimpleExample = new ArrayList<>(Arrays.stream(
                new PostingList[]{new PostingList("example"), new PostingList("simple")}).toList());

        return Stream.of(Arguments.arguments(3, queryPostingsAnotherExample, true, expectedResultsAnotherExampleConjTfidf),
                Arguments.arguments(3, queryPostingsAnotherExample, false, expectedResultsAnotherExampleDisTfidf),
                Arguments.arguments(3, queryPostingsExample, false, expectedResultsExampleDisTfidf),
                Arguments.arguments(3, queryPostingsExample, true, expectedResultsExampleConjTfidf),
                Arguments.arguments(3, queryPostingsSimpleExample, true, expectedResultsEmpty)
        );
    }

    @ParameterizedTest
    @MethodSource("getTFIDFParameters")
    void testMaxScoreTFIDF(int k, ArrayList<PostingList> postings, boolean isConjunctive, PriorityQueue<Map.Entry<Double, Integer>> expected ){
        Flags.setMaxScore(true);
        Flags.setCompression(true);
        Flags.setStemStopRemoval(false);
        assertArrayEquals(reformatQueue(expected), reformatQueue(MaxScore.scoreQuery(postings, k, "tfidf", isConjunctive)));
    }

    @ParameterizedTest
    @MethodSource("getBM25Parameters")
    void testMaxScoreBM25(int k, ArrayList<PostingList> postings, boolean isConjunctive, PriorityQueue<Map.Entry<Double, Integer>> expected) {
        Flags.setMaxScore(true);
        Flags.setCompression(true);
        Flags.setStemStopRemoval(false);
        assertArrayEquals(reformatQueue(expected), reformatQueue(MaxScore.scoreQuery(postings, k, "bm25", isConjunctive)));
    }

    @ParameterizedTest
    @MethodSource("getTFIDFParameters")
    void testDAATTFIDF(int k, ArrayList<PostingList> postings, boolean isConjunctive, PriorityQueue<Map.Entry<Double, Integer>> expected) {
        Flags.setMaxScore(false);
        Flags.setCompression(true);
        Flags.setStemStopRemoval(false);
        assertArrayEquals(reformatQueue(expected), reformatQueue(DAAT.scoreQuery(postings, isConjunctive, k, "tfidf")));
    }

    @ParameterizedTest
    @MethodSource("getBM25Parameters")
    void testDAATBM25(int k, ArrayList<PostingList> postings, boolean isConjunctive, PriorityQueue<Map.Entry<Double, Integer>> expected) {
        Flags.setMaxScore(false);
        Flags.setCompression(true);
        Flags.setStemStopRemoval(false);
        assertArrayEquals(reformatQueue(expected), reformatQueue(DAAT.scoreQuery(postings, isConjunctive, k, "bm25")));
    }

    @AfterAll
    static void teardown() {
        FileUtils.removeFile(VOCABULARY_PATH);
        FileUtils.removeFile(DOCINDEX_PATH);
        FileUtils.removeFile(INVERTED_INDEX_DOCS);
        FileUtils.removeFile(INVERTED_INDEX_FREQS);
        FileUtils.removeFile(BLOCK_DESCRIPTORS_PATH);
    }
}
