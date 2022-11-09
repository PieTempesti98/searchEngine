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
    private static String vocabularyPath;
    private static String invertedIndexPath;

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
            vocabularyPath = doc.getElementsByTagName("vocabularyPath").item(0).getTextContent();
            invertedIndexPath = doc.getElementsByTagName("invertedIndexPath").item(0).getTextContent();

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

    public static String getStopwordsPath() {
        return stopwordsPath;
    }

    public static String getProcessedCollectionPath() {
        return processedCollectionPath;
    }

    public static String geVocabularyPath() {return vocabularyPath;}

    public static String getInvertedIndexPath() {return invertedIndexPath;
    }
}
