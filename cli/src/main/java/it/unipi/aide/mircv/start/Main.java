package it.unipi.aide.mircv.start;

import queryProcessing.QueryProcesser;

import java.util.Scanner;

public class Main {

     /**
      * integer defining the top documents to take into account
      * */
    private static final int k = 10;

    public static void main(String[] args){

        System.out.println("****** SEARCH ENGINE ******");
        System.out.println("Starting...");
        //check is setup of data structures was successful
        boolean setupSuccess = QueryProcesser.setupProcesser();


        if(!setupSuccess){
            System.out.println("Error in setup of this service. Shutting down...");
            return;
        }


        Scanner sc= new Scanner(System.in);

        String query;

        for(;;) {
            System.out.println("What are you looking for? " + """
            Please insert a query specifying your preferred mode:\s
            -c for conjunctive mode or -d for disjunctive mode. Here's an example:\s
            This is a query example -c""");
            query = sc.nextLine();

            //check if user didn't input an empty query
            if(query == null || query.isEmpty())
            {
                System.out.println("The query you entered is empty.");
                continue;
            }

            //get query text and mode.
            // queryInfo[0] -> query text queryInfo[1] -> query mode
            String[] queryInfo = query.split("-");
            if(queryInfo.length < 2){ //check if both text and mode were inputted
                System.out.println("The query you entered is in invalid format.");
                continue;
            }

            //third parameter is true if query mode is conjunctive
            String[] documents = QueryProcesser.processQuery(queryInfo[0],k,queryInfo[1].equals("c"));
            if(documents.length == 0){
                System.out.println("Unfortunately, no documents satisfy your request.");
                continue;
            }

            System.out.println("These documents may do for you:");
            for(String document: documents){

                System.out.println("-> " + document);
            }

        }

    }
}
