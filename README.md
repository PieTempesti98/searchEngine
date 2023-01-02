# SearchEngine 🖥️  :page_with_curl:

Search Engine based on an inverted index developed by Francesca Pezzuti, Pietro Tempesti and Benedetta Tessa for
Multimedia Information Retrieval course at University of Pisa during academic year 2022/2023. The documentation can be found [here](./)

### Project structure

The project is composed by these main modules:

- *CLI*
- *Common module*
- *Indexer*
- *QueryHandler*
- *PerformanceTest*

#### Indexer module

This module performs the indexing of the document collection using the *Spimi* and *Merging* algorithms.

#### CLI module

The *CLI* module is responsible for presenting an interface to the user.

From the interface, the user can input a query and specify some flags as well as the scoring function. (S)he will then
be presented with the top $k$ most relevant documents according to that query.

#### Query Handler module

This module handles the queries received by the CLI module.

In particular, once a query is received, it is pre-processed and tokenized, then the handler retrieves the vocabulary
entries of the tokens and the posting lists and finally applies either *DAAT* or *MaxScore* in order to get a ranking of
the top $k$ most relevant documents for the received query.

#### Common module

This module works as a library: it contains the core data structures and functions needed by all the other modules. It
contains the core classes of the project as well.

#### PerformanceTest module
This module performs tests and writes the results in a format suitable for trec_eval

### How to compile the modules

### Indexer module

The *Indexer* module can be compiled using the following optional flags:

- *-cr* : if specified, it enables **compressed reading** of the document collection from *tar.gz*
- *-c* : if specified, it enables **index compression** using *Unary* for frequencies and *Variable Byte* for docids
- *-s* : if specified, it enables **stopword removal and stemming** during documents' processing
- *-d* : if specified, it enables the execution of the algorithms in **debug mode** allowing the creation of
  human-readable files of the data structure that ca be useful for debbugging purposes.

The choice made for the last three flags will be stored and used for query processing.

If no flags are specified, the indexing will work on the uncompressed document collection (a *tsv* file), the index
won't be compressed, stopwords won't be removed, stemming won't be performed, and debug mode won't be activated.

### Query Handler module

The *Query Handler* module can be compiled using the following optional flags:

- *-maxscore* : if specified, it enables **MaxScore** as dynamic pruning algorithm for query processing

### CLI module

There are no compile flags for this module.

Using the CLI, the user can write a query using either **conjunctive mode** or **disjunctive mode**:

- *-d*: it enables *conjunctive mode*
- *-c*: it enables *disjunctive mode*

The query must be submitted to the system using the following format:

- "query terms \[-d | -c\]"

After having submitted the query, the user is asked to answer whether if (s)he wants to use *TFIDF* or *BM25* as *
*scoring function**, the user must specify either:

- "*tfidf*": it enables *TFIDF* as scoring function
- "*bm25*": it enables *BM25* as scoring function

### Common module

There are no compile flags for this module.


