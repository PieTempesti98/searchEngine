package it.unipi.dii.aide.mircv.common.preprocess;

import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import it.unipi.dii.aide.mircv.common.config.Flags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PreprocesserTest {

    @BeforeAll
    static void readStopwords() {
        Preprocesser.setTestPath();
        Preprocesser.readStopwords();
    }


    public static Stream<Arguments> getRawTextAndCleanText() {

        return Stream.of(arguments("this is the url of university of Pisa \n" +
                        "https://www.unipi.it", "this is the url of university of Pisa"),
                arguments("<p> 1343 is the year it was founded </p>", "is the year it was founded"),
                arguments("another      test", "another test"));
    }

    public static Stream<Arguments> getTextWithAndWithoutStopwords() {

        return Stream.of(  arguments("to be or not to be", ""),
                arguments("this test should return true", "test return true"),
                arguments("this sentence contains some stopwords", "sentence stopwords"));
    }

    public static Stream<Arguments> getTextToProcessAndProcessed() {

        return Stream.of(  arguments("my package never arrived https://www.amazon.com/gp/css/order-history?ref_=nav_orders_first please help!", "my package never arrived please help"),
                arguments("This is soooo cool!!!   AndUnexpected", "this is soo cool and unexpected"),
                arguments("<body> This is a body, tags should be removed </body>", "this is a body tags should be removed"));
    }

    public static Stream<Arguments> getTextToProcessAndProcessedStopStem() {

        return Stream.of(  arguments("my package never arrived https://www.amazon.com/gp/css/order-history?ref_=nav_orders_first please help!", "packag arriv help"),
                arguments("This is soooo cool!!!   AndUnexpected", "soo cool unexpect"),
                arguments("<body> This is a body, tags should be removed </body>", "bodi tag remov"));
    }

    public static Stream<Arguments> getRawTextAndTokens() {


        return Stream.of(  arguments("", new String[]{""}),
                arguments("OnlyCamelCase", new String[]{"only","camel","case"}),
                arguments("only white spaces", new String[]{"only","white","spaces"}),
                arguments("ThisIs a Combination OfBoth", new String[]{"this","is","a","combination","of","both"}));

    }

    @ParameterizedTest
    @MethodSource("getRawTextAndTokens")
    void tokenizedText_ShouldReturnCorrectTokens(String rawText, String[] expectedTokens) {
        assertArrayEquals(expectedTokens, Preprocesser.tokenize(rawText));
    }

    @ParameterizedTest
    @MethodSource("getRawTextAndCleanText")
    void cleanText_ShouldReturnCleanedText(String rawText, String cleanText) {
        assertEquals(cleanText, Preprocesser.cleanText(rawText));
    }

    @ParameterizedTest
    @MethodSource("getTextWithAndWithoutStopwords")
    void removeStopwords_ShouldReturnTextWithoutStopwords(String textWithStopwords, String textWithoutStopwords) {
        Flags.setStemStopRemoval(false);
        String[] actualResult = Preprocesser.removeStopwords(textWithStopwords.split(" "));
        assertEquals(textWithoutStopwords, String.join(" ", actualResult));
    }

    @ParameterizedTest
    @MethodSource("getTextToProcessAndProcessed")
    void processDocument_StemmingAndStopWordRemovalDisabled(String textToProcess, String textProcessed) {
        Flags.setStemStopRemoval(false);
        ProcessedDocument actualDocument = Preprocesser.processDocument(new TextDocument("_",textToProcess));
        assertEquals(actualDocument.getTokens(), List.of(textProcessed.split(" ")));
    }

    @ParameterizedTest
    @MethodSource("getTextToProcessAndProcessedStopStem")
    void processDocument_StemmingAndStopWordRemovalEnabled(String textToProcess, String textProcessed) {
        Flags.setStemStopRemoval(true);
        ProcessedDocument actualDocument = Preprocesser.processDocument(new TextDocument("_",textToProcess));
        assertEquals(actualDocument.getTokens(), List.of(textProcessed.split(" ")));
    }
}