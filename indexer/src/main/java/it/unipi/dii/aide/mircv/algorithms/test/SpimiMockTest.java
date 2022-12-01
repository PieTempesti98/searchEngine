package it.unipi.dii.aide.mircv.algorithms.test;


import it.unipi.dii.aide.mircv.beans.DocumentIndexEntry;
import it.unipi.dii.aide.mircv.common.beans.ProcessedDocument;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SpimiMockTest {

    public static Stream<Arguments> getDocumentIndex() {

        return Stream.of(
                arguments(new HashMap<>(),new ArrayList<>())
        );
    }

    @ParameterizedTest
    @MethodSource("getDocumentIndex")
    void buildDocumentIndex_ShouldReturnDocumentIndex(HashMap<Integer, DocumentIndexEntry> expectedDocumentIndex,
                                                      ArrayList<ProcessedDocument> testDocuments) {
        assertEquals(expectedDocumentIndex, SpimiMock.buildDocumentIndex(testDocuments));
    }

    public static Stream<Arguments> getPartialIndex() {

        return Stream.of(
                arguments(new HashMap<>(),new ArrayList<>())
        );
    }

    @ParameterizedTest
    @MethodSource("getPartialIndex")
    void buildPartialIndex_ShouldReturnPartialIndex(HashMap<Integer, DocumentIndexEntry> expectedPartialIndex,
                                                      ArrayList<ProcessedDocument> testDocuments) {
        assertEquals(expectedPartialIndex, SpimiMock.executeSpimiMock(testDocuments));
    }

}