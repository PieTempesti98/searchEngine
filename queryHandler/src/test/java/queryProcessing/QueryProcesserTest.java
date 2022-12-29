package queryProcessing;

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


class QueryProcesserTest {

    private static Vocabulary v = Vocabulary.getInstance();
    private static DocumentIndex docIndex = DocumentIndex.getInstance();
    static HashMap<String, PostingList> index = new LinkedHashMap<>();

    public static Stream<Arguments> getParameters() {

        //queue for query "another example" conjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleConjTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.8061799739838872, 1));
        expectedResultsAnotherExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.9874180905628003, 7));

        //queue for query "another example" disjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsAnotherExampleDisTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.8061799739838872, 1));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 5));
        expectedResultsAnotherExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.9874180905628003, 7));

        //postings for query "another example"
        PostingList pl = new PostingList("example	1:1 2:1 4:1 5:3 7:1");
        PostingList pl1 = new PostingList("another	1:1 7:2");
        ArrayList<PostingList> queryPostingsAnotherExample = new ArrayList<>();
        queryPostingsAnotherExample.add(pl);
        queryPostingsAnotherExample.add(pl1);

        //queue for query "example" disjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleDisTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 4));
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 2));
        expectedResultsExampleDisTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 5));

        //queue for query "example" conjunctive mode with tfidf
        PriorityQueue<Map.Entry<Double, Integer>> expectedResultsExampleConjTfidf = new PriorityQueue<>(3, Map.Entry.comparingByKey());
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.2041199826559248, 4));
        expectedResultsExampleConjTfidf.add(new AbstractMap.SimpleEntry<>(0.30150996489407533, 5));

        //postings for query "example"
        ArrayList<PostingList> queryPostingsExample = new ArrayList<>();
        queryPostingsExample.add(pl);

        return Stream.of(Arguments.arguments("tfidf",3,queryPostingsAnotherExample,true,expectedResultsAnotherExampleDisTfidf),
                Arguments.arguments("tfidf",3,queryPostingsAnotherExample,false,expectedResultsAnotherExampleConjTfidf),
                Arguments.arguments("tfidf",3,queryPostingsExample,false,expectedResultsExampleDisTfidf),
                Arguments.arguments("tfidf",3,queryPostingsExample,true,expectedResultsExampleConjTfidf)
        );
    }

    @BeforeAll
    public static void init(){

        CollectionSize.setTotalDocLen(64);
        CollectionSize.setCollectionSize(11);

        v.setVocabularyPath("../data/vocabulary");
        VocabularyEntry.setBlockDescriptorsPath("../data/blockDescriptors");
        boolean success = v.readFromDisk();
        System.out.println(v);
        assertTrue(success);

        DocumentIndexEntry.setDocindexPath("../data/documentIndex");
        success = docIndex.loadFromDisk();
        assertTrue(success);


    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void testDAAT(String scoringFunction,int k,ArrayList<PostingList> postings,boolean isConjunctive,PriorityQueue<Map.Entry<Double, Integer>> expected ){
        Flags.setMaxScore(false);
        assertEquals(expected, DAAT.scoreQuery(postings,isConjunctive,k,scoringFunction));
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void testMaxScore(String scoringFunction,int k,ArrayList<PostingList> postings,boolean isConjunctive,PriorityQueue<Map.Entry<Double, Integer>> expected ){
        Flags.setMaxScore(true);
        assertEquals(expected, MaxScore.scoreQuery(postings,k,scoringFunction,isConjunctive));
    }


}
