package it.unipi.dii.aide.mircv.utils;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.removeFile;

public class Utility {
    private static final String DOC_INDEX_PATH = ConfigurationParameters.getDocumentIndexPath();

    private static final String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();

    private static final String PARTIAL_INDEX_PATH = ConfigurationParameters.getPartialIndexPath();

    //private static final String INVERTED_INDEX_PATH = ConfigurationParameters.getInvertedIndexPath();

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
        removeFile(PARTIAL_INDEX_PATH);

        //TODO: update
        /*
        removeFile(INVERTED_INDEX_PATH);
        */
        Preprocesser.readStopwords();
    }




}
