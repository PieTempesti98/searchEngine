package it.unipi.dii.aide.mircv.common.preprocess;

import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import it.unipi.dii.aide.mircv.common.beans.TextDocument;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PreprocesserTest {
/*
    @BeforeAll
    public static void init(){
        Preprocesser.readStopwords();
    }

    public static Stream<Arguments> getRawTextAndCleanText() {

    return Stream.of(  arguments("this is the url of university of Pisa \n" +
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

        return Stream.of(  arguments("", ""),
                arguments("", ""),
                arguments("", ""));
    }

    @ParameterizedTest
    @MethodSource("getRawTextAndCleanText")
    void cleanText_ShouldReturnCleanedText(String rawText, String cleanText) {
        assertEquals(cleanText, Preprocesser.cleanText(rawText));
    }

    @ParameterizedTest
    @MethodSource("getTextWithAndWithoutStopwords")
    void removeStopwords_ShouldReturnTextWithoutStopwords(String textWithStopwords, String textWithoutStopwords) {
        String[] actualResult = Preprocesser.removeStopwords(textWithStopwords.split(" "));
        assertEquals(textWithoutStopwords, String.join(" ", actualResult));
    }

    @ParameterizedTest
    @MethodSource("getTextToProcessAndProcessed")
    void processDocument_ShouldReturnDocumentAppropiatelyProcessed(String textToProcess, String textProcessed) {
        ProcessedDocument actualDocument = Preprocesser.processDocument(new TextDocument("_",textToProcess));
        assertEquals(actualDocument.getTokens(), List.of(textProcessed.split(" ")));
    }*/
}