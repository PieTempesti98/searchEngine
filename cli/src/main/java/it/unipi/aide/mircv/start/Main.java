package it.unipi.aide.mircv.start;

import queryProcessing.QueryProcesser;

import java.sql.Timestamp;
import java.time.LocalDateTime;
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

        System.out.println("What are you looking for? " + """
            Please insert a query specifying your preferred mode:\s
            -c for conjunctive mode or -d for disjunctive mode. Here's an example:\s
            This is a query example -c \s
            Type "help" to get help or "break" to terminate the service""");

        for(;;) {
            System.out.println("What are you looking for? Type \"help\" to get help or \"break\" to terminate the service to terminate the service");
            query = sc.nextLine();

            //check if user didn't input an empty query
            if(query == null || query.isEmpty())
            {
                System.out.println("The query you entered is empty.");
                continue;
            }

            //get query text and mode.
            // queryInfo[0] -> query text or break or help , queryInfo[1] -> query mode
            String[] queryInfo = query.split("-");

            if(queryInfo.length == 1){

                //check if break input is inputted
                if(queryInfo[0].equals("break")){
                    System.out.println("Bye.. Hope you have found everything you were looking for :)");
                    break;
                }

                //check if help input is inputted
                if(queryInfo[0].equals("help")){
                    System.out.println("""
                            Please insert a query specifying your preferred mode:
                                        -c for conjunctive mode or -d for disjunctive mode. Here's an example:s
                                        This is a query example -c s
                                        Insert -h for help or -b to terminate the service""");
                    continue;
                }

                //invalid input
                System.out.println("The query you entered is in invalid format.");
                continue;
            }

            //check if query mode is valid
            if(!queryInfo[1].equals("c") && !queryInfo[1].equals("d")){
                System.out.println("The query you entered is in invalid format.");
                continue;
            }

            //third parameter is true if query mode is conjunctive
            long start = System.currentTimeMillis();
            String[] documents = QueryProcesser.processQuery(queryInfo[0],k,queryInfo[1].equals("c"));
            long stop = System.currentTimeMillis();
            if(documents == null){
                System.out.println("Unfortunately, no documents satisfy your request.");
                continue;
            }

            System.out.println("These documents may do for you, in " + (stop - start) + "ms:");
            for(String document: documents){
                if(document != null)
                    System.out.println("-> " + document);
            }

        }

    }
}
