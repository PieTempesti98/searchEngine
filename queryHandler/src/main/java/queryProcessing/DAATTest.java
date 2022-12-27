package queryProcessing;

import it.unipi.dii.aide.mircv.common.beans.DocumentIndex;
import it.unipi.dii.aide.mircv.common.beans.PostingList;
import it.unipi.dii.aide.mircv.common.beans.Vocabulary;
import it.unipi.dii.aide.mircv.common.config.CollectionSize;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.*;


class DAATTest {

    static Vocabulary v = Vocabulary.getInstance();
    DocumentIndex dI= DocumentIndex.getInstance();
    HashMap<String, PostingList> index = new LinkedHashMap<>();

    public static Stream<Arguments> getParameters() {

        return Stream.of(Arguments.arguments("bM25",1,"adam and eve",0.0),
                Arguments.arguments("bM25",5,"adam and eve",0.0),
                Arguments.arguments("bM25",1,"fruit salad recipe",0.0),
                Arguments.arguments("tfidf",1,"adam and eve",0.0),
                Arguments.arguments("tfidf",5,"adam sandler new movie",0.0),
                Arguments.arguments("tfidf",1,"fruit salad recipe",0.0)

                );
    }

    @BeforeAll
    public static void init(){

        CollectionSize.updateDocumentsLenght(64);
        CollectionSize.updateCollectionSize(11);

        v.readFromDisk("../data/testData/testVoc");


    }

    @ParameterizedTest
    @MethodSource("getParameters")
    void testDAAT(String scoringFunction,int k, String query,double score){

        assertNotNull(QueryProcesser.processQuery("adam",2,false,"tfidf"));
    }




}