import it.unipi.dii.aide.mircv.common.beans.*;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import it.unipi.dii.aide.mircv.common.config.Flags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import queryProcessing.DAAT;
import queryProcessing.MaxScore;

import java.util.*;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;


class DAATTest {

    private static Vocabulary v = Vocabulary.getInstance();
    private static DocumentIndex docIndex = DocumentIndex.getInstance();

    public static Stream<Arguments> getParameters() {

        //queue for query "another example" conjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleConjTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(1.1051215790680997, 8));
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.9084850188786497, 2));

        //queue for query "another example" disjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleDisTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(1.1051215790680997, 8));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.9084850188786497, 2));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.37706844303362685, 6));


        // <------ Posting Lists Creation ------>

        //postings for query "another example"
        PostingList plAnother = new PostingList("another	2:1 8:2");
        PostingList plExample = new PostingList("example	2:1 3:1 5:1 6:3 8:1");
        PostingList plSimple = new PostingList("simple	1:1 7:1");

        //postings for query "example"
        ArrayList<PostingList> queryPostingsExample = new ArrayList<>();
        queryPostingsExample.add(plExample);

        ArrayList<PostingList> queryPostingsSimpleExample = new ArrayList<>();
        queryPostingsSimpleExample.add(plExample);
        queryPostingsSimpleExample.add(plSimple);

        ArrayList<PostingList> queryPostingsAnotherExample = new ArrayList<>();
        queryPostingsAnotherExample.add(plAnother);
        queryPostingsAnotherExample.add(plExample);

        //queue for query "example" disjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleDisTfidf = new PriorityQueue<>(2, Map.Entry.comparingByKey());
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.37706844303362685, 6));
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.25527250510330607, 3));

        //queue for query "example" conjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleConjTfidf = new PriorityQueue<>(2, Map.Entry.comparingByKey());
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.37706844303362685, 6));
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.25527250510330607, 3));


        // <----------- BM25 --------->

        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleDisBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.43001036781065094, 2));
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.14044304677611427, 3));
        expectedResultsAnotherExampleDisBM25.add(new AbstractMap.SimpleEntry<>(0.2896621498549027, 8));

        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleConjBM25 = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.43001036781065094, 2));
        expectedResultsAnotherExampleConjBM25.add(new AbstractMap.SimpleEntry<>(0.2896621498549027, 8));

        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleBM25 = new PriorityQueue<>(4, Map.Entry.comparingByKey());
        expectedResultsExampleBM25.add(new AbstractMap.SimpleEntry<>(0.12082733510224382, 2));
        expectedResultsExampleBM25.add(new AbstractMap.SimpleEntry<>(0.14044304677611427, 3));
        expectedResultsExampleBM25.add(new AbstractMap.SimpleEntry<>(0.11294014731678455, 5));
        expectedResultsExampleBM25.add(new AbstractMap.SimpleEntry<>(0.09624752325208719, 6));


        return Stream.of(Arguments.arguments("tfidf",3,queryPostingsAnotherExample,true,expectedResultsAnotherExampleConjTfidf),
                Arguments.arguments("tfidf",3,queryPostingsAnotherExample,false,expectedResultsAnotherExampleDisTfidf),
                Arguments.arguments("tfidf",3,queryPostingsSimpleExample,true,new PriorityQueue<>(3, Map.Entry.comparingByKey())),
                Arguments.arguments("bm25",3,queryPostingsAnotherExample,false,expectedResultsAnotherExampleDisBM25),
                Arguments.arguments("bm25",3,queryPostingsAnotherExample,true,expectedResultsAnotherExampleConjBM25),
                Arguments.arguments("bm25",4,queryPostingsExample,false,expectedResultsExampleBM25)
                );
    }

    @BeforeAll
    public static void init(){

        CollectionSize.setTotalDocLen(61);
        CollectionSize.setCollectionSize(8);

        Vocabulary.setVocabularyPath("../data/vocabulary");
        VocabularyEntry.setBlockDescriptorsPath("../data/blockDescriptors");

        DocumentIndexEntry.setDocindexPath("../data/documentIndex");

        BlockDescriptor.setInvertedIndexDocs("../data/InvertedIndexDocs");
        BlockDescriptor.setInvertedIndexFreqs("../data/InvertedIndexFreqs");


    }
    /**
     * utility function converting a priority queue into a sorted arrayList
     * @param queue the priority queue to convert
     * @return sorted array
     */
    public Object[] reformatQueue(PriorityQueue<Map.Entry<Double, Integer>> queue){

        //arraylist storing the result
        ArrayList<AbstractMap.SimpleEntry<Double,Integer>> returnList = new ArrayList<>();

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


    @ParameterizedTest
    @MethodSource("getParameters")
    void testDAAT(String scoringFunction,int k,ArrayList<PostingList> postings,boolean isConjunctive,PriorityQueue<Map.Entry<Double, Integer>> expected ){

        boolean success = v.readFromDisk();
        assertTrue(success);

        success = docIndex.loadFromDisk();
        assertTrue(success);


        //convert priority queue to array
        assertArrayEquals(reformatQueue(expected),reformatQueue(DAAT.scoreQuery(postings,isConjunctive,k,scoringFunction)));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void testMaxScore(String scoringFunction,int k,ArrayList<PostingList> postings,boolean isConjunctive,PriorityQueue<Map.Entry<Double, Integer>> expected ){
        Flags.setMaxScore(true);

        boolean success = v.readFromDisk();
        assertTrue(success);

        success = docIndex.loadFromDisk();
        assertTrue(success);


        assertEquals(expected, MaxScore.scoreQuery(postings,k,scoringFunction,isConjunctive));
    }


}