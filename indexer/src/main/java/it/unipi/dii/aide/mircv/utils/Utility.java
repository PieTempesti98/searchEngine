package it.unipi.dii.aide.mircv.utils;

import it.unipi.dii.aide.mircv.common.config.ConfigurationParameters;
import it.unipi.dii.aide.mircv.common.config.Flags;
import it.unipi.dii.aide.mircv.common.preprocess.Preprocesser;
import it.unipi.dii.aide.mircv.common.utils.FileUtils;

import static it.unipi.dii.aide.mircv.common.utils.FileUtils.*;

/**
 * utility class for the indexing algorithms
 */
public class Utility {
    private static final String DOC_INDEX_PATH = ConfigurationParameters.getDocumentIndexPath();
    private static final String VOCABULARY_PATH = ConfigurationParameters.getVocabularyPath();
    private static final String PARTIAL_INDEX_DOCIDS = ConfigurationParameters.getDocidsDir();
    private static final String PARTIAL_INDEX_FREQS = ConfigurationParameters.getFrequencyDir();
    private static final String INVERTED_INDEX_DOCIDS = ConfigurationParameters.getInvertedIndexDocs();
    private static final String INVERTED_INDEX_FREQS = ConfigurationParameters.getInvertedIndexFreqs();
    private static final String PARTIAL_VOCABULARY_PATH = ConfigurationParameters.getPartialVocabularyDir();
    private static final String BLOCK_DESCRIPTORS_PATH = ConfigurationParameters.getBlockDescriptorsPath();

    /**
     * deletes the older version of the inverted index and possible intermediate indexes,
     * and creates new folders for the partial indexes
     */
    public static void initializeFiles() {

        removeFile(DOC_INDEX_PATH);
        removeFile(VOCABULARY_PATH);
        removeFile(INVERTED_INDEX_DOCIDS);
        removeFile(INVERTED_INDEX_FREQS);
        removeFile(BLOCK_DESCRIPTORS_PATH);

        deleteDirectory(PARTIAL_INDEX_DOCIDS);
        deleteDirectory(PARTIAL_INDEX_FREQS);
        deleteDirectory(PARTIAL_VOCABULARY_PATH);
        deleteDirectory("data/debug");

        //create directories to store partial frequencies, docids and vocabularies
        createDirectory(ConfigurationParameters.getDocidsDir());
        createDirectory(ConfigurationParameters.getFrequencyDir());
        createDirectory(ConfigurationParameters.getPartialVocabularyDir());

        if (Flags.isStemStopRemovalEnabled())
            Preprocesser.readStopwords();
    }

    /**
     * deletes the folders of the partial indexes when the indexing is done
     */
    public static void cleanUpFiles() {
        // remove partial index docids directory
        FileUtils.deleteDirectory(ConfigurationParameters.getDocidsDir());

        // remove partial index frequencies directory
        FileUtils.deleteDirectory(ConfigurationParameters.getFrequencyDir());

        // remove partial vocabularies directory
        FileUtils.deleteDirectory(ConfigurationParameters.getPartialVocabularyDir());
    }
}
