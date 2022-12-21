package it.unipi.dii.aide.mircv.utils;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.deleteDirectory;
import static it.unipi.dii.aide.mircv.common.utils.FileUtils.removeFile;

public class Utility {
    private static final String DOC_INDEX_PATH = ConfigurationParameters.getDocumentIndexPath();

    private static final String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();

    private static final String PARTIAL_INDEX_DOCIDS = ConfigurationParameters.getDocidsDir();
    private static final String PARTIAL_INDEX_FREQS = ConfigurationParameters.getFrequencyDir();

    private static final String INVERTED_INDEX_DOCIDS = ConfigurationParameters.getInvertedIndexDocs();
    private static final String INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqs();
    private static final String PARTIAL_VOCABULARY_PATH = ConfigurationParameters.getPartialVocabularyDir();


    /**
     * Number of different intermediate indexes
     */
    private static int numIndexes = 0;

    public static int getNumIndexes() {
        return numIndexes;
    }

    public static void setNumIndexes(int numIndexes) {
        Utility.numIndexes = numIndexes;
    }

    public static void initializeFiles(){
        removeFile(DOC_INDEX_PATH);
        removeFile(VOCABULARY_PATH);
        removeFile(INVERTED_INDEX_DOCIDS);
        removeFile(INVERTED_INDEX_FREQS);
        deleteDirectory(PARTIAL_INDEX_DOCIDS);
        deleteDirectory(PARTIAL_INDEX_FREQS);
        deleteDirectory(PARTIAL_VOCABULARY_PATH);
        deleteDirectory("data/debug");

        //TODO: update
        /*
        removeFile(INVERTED_INDEX_PATH);
        */
        Preprocesser.readStopwords();
    }




}
