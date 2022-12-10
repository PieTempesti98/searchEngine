package it.unipi.dii.aide.mircv.common.config;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

public class ConfigurationParameters {
    private static String rawCollectionPath;
    private static String loadedCollectionPath;
    private static String stopwordsPath;
    private static String processedCollectionPath;
    private static String documentIndexPath;
    private static String partialIndexPath;
    private static String vocabularyPath;
    private static String invertedIndexPath;
    private static String partialVocabularyDir;
    private static String frequencyFileName;
    private static String docidsFileName;
    private static String vocabularyFileName;
    private static String frequencyDir;
    private static String docidsDir;


    static {
        try{

            // create the document builder and parse the configuration file
            File file = new File("config/config.xml");
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);

            // with the "normalize" method the Text nodes are only separated by the structure
            // There are neither adjacent text nodes nor empty text nodes
            doc.getDocumentElement().normalize();

            // retrieve the configuration information
            rawCollectionPath = doc.getElementsByTagName("rawCollectionPath").item(0).getTextContent();
            loadedCollectionPath = doc.getElementsByTagName("loadedCollectionPath").item(0).getTextContent();
            stopwordsPath = doc.getElementsByTagName("stopwordsPath").item(0).getTextContent();
            processedCollectionPath = doc.getElementsByTagName("processedCollectionPath").item(0).getTextContent();
            documentIndexPath = doc.getElementsByTagName("documentIndexPath").item(0).getTextContent();
            //partialIndexPath = doc.getElementsByTagName("partialIndexPath").item(0).getTextContent();
            vocabularyPath = doc.getElementsByTagName("vocabularyPath").item(0).getTextContent();
            invertedIndexPath = doc.getElementsByTagName("invertedIndexPath").item(0).getTextContent();
            partialVocabularyDir = doc.getElementsByTagName("partialVocabularyDir").item(0).getTextContent();
            frequencyFileName = doc.getElementsByTagName("frequencyFileName").item(0).getTextContent();
            docidsFileName = doc.getElementsByTagName("docidsFileName").item(0).getTextContent();
            vocabularyFileName = doc.getElementsByTagName("vocabularyFileName").item(0).getTextContent();
            frequencyDir = doc.getElementsByTagName("frequencyDir").item(0).getTextContent();
            docidsDir = doc.getElementsByTagName("docidsDir").item(0).getTextContent();


        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static String getRawCollectionPath() {
        return rawCollectionPath;
    }

    public static String getLoadedCollectionPath() {
        return loadedCollectionPath;
    }

    public static String getStopwordsPath() {return stopwordsPath;}

    public static String getProcessedCollectionPath() {
        return processedCollectionPath;
    }

    public static String getDocumentIndexPath() {return documentIndexPath;}

    public static String getPartialIndexPath() {return partialIndexPath;}

    public static String getVocabularyPath() {return vocabularyPath;}

    public static String getInvertedIndexPath() {return invertedIndexPath;}

    public static String getPartialVocabularyDir() {return partialVocabularyDir;}

    public static String getFrequencyFileName() {return frequencyFileName;}

    public static String getDocidsFileName() {return docidsFileName;}

    public static String getFrequencyDir() {return frequencyDir;}

    public static String getDocidsDir() {return docidsDir;}

    public static String getVocabularyFileName() {return vocabularyFileName;}
}
