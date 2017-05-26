/*
 *  Copyright (c) 2017, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.1.2.
 */
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *  This software illustrates the architecture for the portion of a
 *  search engine that evaluates queries.  It is a guide for class
 *  homework assignments, so it emphasizes simplicity over efficiency.
 *  It implements an unranked Boolean retrieval model, however it is
 *  easily extended to other retrieval models.  For more information,
 *  see the ReadMe.txt file.
 */
public class QryEval {

  //  --------------- Constants and variables ---------------------
  private static final String USAGE =
    "Usage:  java QryEval paramFile\n\n";

  private static final String[] TEXT_FIELDS =
    { "body", "title", "url", "inlink" };


  //  --------------- Methods ---------------------------------------

  /**
   *  @param args The only argument is the parameter file name.
   *  @throws Exception Error accessing the Lucene index.
   */
  public static void main(String[] args) throws Exception {

    //  This is a timer that you may find useful.  It is used here to
    //  time how long the entire program takes, but you can move it
    //  around to time specific parts of your code.
    
//    Timer timer = new Timer();
//    timer.start ();

    //  Check that a parameter file is included, and that the required
    //  parameters are present.  Just store the parameters.  They get
    //  processed later during initialization of different system
    //  components.

    if (args.length < 1) {
      throw new IllegalArgumentException (USAGE);
    }

    Map<String, String> parameters = readParameterFile (args[0]);


    //  Open the index and initialize the retrieval model.

    Idx.open (parameters.get ("indexPath"));
    RetrievalModel model = initializeRetrievalModel (parameters);

    // Call learning to rank class
    if (model instanceof RetrievalModelLETOR){

      LearningToRank obj = new LearningToRank(parameters,true);

      //fit and transform model on training data
      obj.fit_transform();
      obj.transform();

      // predict value of test data

    }else{
      processQueryFileNew(parameters, model);
    }


    //  Perform experiments.
    
    //processQueryFile(parameters.get("queryFilePath"), model);
    //  Clean up.
    
//    timer.stop ();
//    System.out.println ("Time:  " + timer);
  }


  static void processQueryFileNew(Map<String, String> parameters,
                               RetrievalModel model)
          throws IOException {

    BufferedReader input = null;

    try {
      String qLine = null;

      input = new BufferedReader(new FileReader(parameters.get("queryFilePath")));

      // out is used to write output result for each query to a file
      // expanded_out is used to write the expanded query terms to a file
      PrintWriter out = new PrintWriter(parameters.get("trecEvalOutputPath"));
      PrintWriter expanded_out = null;

      if (parameters.containsKey("fbExpansionQueryFile")){
        expanded_out = new PrintWriter(parameters.get("fbExpansionQueryFile"));
      }

      while ((qLine = input.readLine()) != null) {

        int d = qLine.indexOf(':');
        if (d < 0) {
          throw new IllegalArgumentException
                  ("Syntax error:  Missing ':' in query line.");
        }
        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);
        ScoreList r = null;

        // based on value of fbexpansion check if expansion is needed or not
        if (!parameters.containsKey("fb") || parameters.get("fb").equals("false")){
          r = processQuery(query, model);
        }else{
          r = getExpandedRanking(parameters, qid, expanded_out, query, model);
        }

        // perform diversification
        if (!parameters.containsKey("diversity") || parameters.get("diversity").equals("false")){
          r = processQuery(query, model);
        }else{
          r = getDiversifiedRanking(parameters, qid, query, model);

        }



        try {
          if (r != null) {
            r.sort();
            if (r.size() < 1) {
              out.print(qid + " Q0 " + "dummy" + " " + 1 +" "+ 0 + " agaur\n");
            } else {
              int max_size = 100;
              int loop = max_size < r.size() ? max_size : r.size();
              for (int i = 0; i < loop; i++) {
                out.print(qid + " Q0 " + Idx.getExternalDocid(r.getDocid(i)) + " " + (i+1) +" "+ r.getDocidScore(i) + " agaur\n");
              }
            }
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (parameters.containsKey("fbExpansionQueryFile")) {
        expanded_out.close();
      }
      out.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      input.close();
    }
  }

  private static ScoreList getDiversifiedRanking(Map<String, String> parameters, String qid, String query, RetrievalModel model) throws IOException {


    ScoreList r = null;

    // check which reference system is to be used for getting documents to diversify query
    try{

      HashMap<String, ScoreList> qiScore = new HashMap<>();
      ArrayList<String> queryIntents = new ArrayList<>();
      if (parameters.containsKey("diversity:initialRankingFile")) {

        r = new ScoreList();

        String line = null;
        String[] pair = null;
        BufferedReader docFile = new BufferedReader(new FileReader(parameters.get("diversity:initialRankingFile")));
        while ((line = docFile.readLine()) != null) {

          pair = line.split(" ");
          String currqid;
          if (pair[0].contains(".")) {
            currqid = pair[0].split("\\.")[0];
          } else {
            currqid = pair[0];
          }

          if (qid.equals(currqid)) {
            if (!qiScore.containsKey(pair[0])) {
              r = new ScoreList();
              qiScore.put(pair[0], r);
              if ( !queryIntents.contains(pair[0])){
                queryIntents.add(pair[0]);
              }
            }
            r.add(Idx.getInternalDocid(pair[2]), Double.parseDouble(pair[4]));
          }
        }
      }else{

        queryIntents.add(query);
        BufferedReader intentFile = new BufferedReader(new FileReader(parameters.get("diversity:intentsFile")));
        String qIntent;
        while ((qIntent = intentFile.readLine()) != null) {
          String[] pair = qIntent.split("\\.");
          if ( pair[0].equals(qid)){
            int d = qIntent.indexOf(':');
            queryIntents.add(qIntent.substring(d + 1));
          }

        }
        for (String q : queryIntents){
          r = processQuery(q, model);
          qiScore.put(q, r);
        }
      }

      // create hashmap to store docid and score for each intent for top k docs

      HashMap<Integer, double[]> docScore = new HashMap<>();
      ScoreList qorg = qiScore.get(queryIntents.get(0));
      double[] maxSum = new double[qiScore.size()];
      boolean greaterthan1 = false;

      int maxDoc = Integer.parseInt(parameters.get("diversity:maxInputRankingsLength"));
      int loopvar = maxDoc < qorg.size() ? maxDoc : qorg.size();
      for ( int i = 0; i < loopvar; i++){
        double[] score = new double[qiScore.size()];
        int docID = qorg.getDocid(i);
        score[0] = qorg.getDocidScore(i) ;
        docScore.put(docID, score );
        maxSum[0] += score[0];
        if (score[0] > 1.0){
          greaterthan1 = true;
        }
      }
      double max = maxSum[0];

      for (int j = 1; j < qiScore.size(); j++){
        ScoreList qi = qiScore.get(queryIntents.get(j));
        for ( int i = 0; i < loopvar; i++) {
          if ( i == qi.size()) {
            break;
          }
          int docID = qi.getDocid(i);
          if (docScore.containsKey(docID)){
            double[] score = docScore.get(docID);
            score[j] = qi.getDocidScore(i);
            maxSum[j] += score[j];
            if (score[j] > 1.0) {
              greaterthan1 = true;
            }
          }
        }
        if ( maxSum[j] > max){
          max = maxSum[j];
        }
      }

      if (greaterthan1){
        for (int doc : docScore.keySet()){
          double[] score = docScore.get(doc);
          for (int i =0; i < score.length ; i++){
            score[i] /= max;
          }
        }
      }


      // use diversification algorithm
      r = new ScoreList();
      double lambda = Double.parseDouble(parameters.get("diversity:lambda"));
      if (parameters.get("diversity:algorithm").equalsIgnoreCase("xquad")){

        HashMap<Integer,double[]> docset = new HashMap<>();
        int resSize = Integer.parseInt(parameters.get("diversity:maxResultRankingLength"));
        int maxSize = docScore.size() < resSize ? docScore.size(): resSize;
        while ( docset.size() < maxSize) {

          double prob_qiq   = 1.0 / (queryIntents.size() - 1);
          getXquadMaxScoreList(docScore, docset, lambda, prob_qiq, r);
        }
      }else {
        // use PM2 for diversification
        HashMap<Integer,double[]> docset = new HashMap<>();
        int resSize = Integer.parseInt(parameters.get("diversity:maxResultRankingLength"));
        int maxSize = docScore.size() < resSize ? docScore.size(): resSize;
        double[] v = new double[queryIntents.size() - 1];
        double[] s = new double[queryIntents.size() - 1];
        double[] qt = new double[queryIntents.size() - 1];
        Arrays.fill(v, maxSize / (queryIntents.size() - 1.0));

        while(docset.size() < maxSize) {
          double maxq = -1.0;
          int maxidx = 0;
          for (int i = 0; i < qt.length; i++){
            qt[i] = v[i] / (2 * s[i] + 1);
            if (qt[i] > maxq){
              maxq = qt[i];
              maxidx = i;
            }
          }
          double maxscore = 0.0;
          int maxdoc = 0;
          for (int doc : docScore.keySet()){
            double probdq = docScore.get(doc)[maxidx + 1];
            double score = lambda * qt[maxidx] * probdq;

            for(int i = 0; i < qt.length; i++){
              if (i != maxidx){
                double probdjqi = docScore.get(doc)[i+1];
                score += ((1 - lambda) * qt[i] * probdjqi);
              }

            }
            if (maxscore < score){
              maxscore = score;
              maxdoc = doc;
            }
          }

          double total = 0.0;
          if (maxscore == 0.0){
            break;
          }
          for (int i = 0; i < s.length; i++){
            total += docScore.get(maxdoc)[i+1];
          }

          for (int i = 0; i < s.length; i++){
            s[i] += docScore.get(maxdoc)[i+1]/total;
          }

          double[] sc = docScore.get(maxdoc);
          docset.put(maxdoc, sc);
          docScore.remove(maxdoc);
          r.add(maxdoc, maxscore);
        }
        if (r.size() < maxSize){
          for ( int doc : docScore.keySet()){
            if ( r.size() < maxSize ) {
              r.add(doc, docScore.get(doc)[0]);
            }

          }
        }
      }
      }catch (Exception ex){
      ex.printStackTrace();
    }
    return r;

  }

  private static void getXquadMaxScoreList(HashMap<Integer, double[]> docscore,HashMap<Integer,double[]> docset, double lambda, double prob_qiq, ScoreList r ) {

    double max = -1.0;
    int maxdocid = 0;
    for (int doc : docscore.keySet()) {

      double prob_dq = docscore.get(doc)[0];

      double score = (1 - lambda) * prob_dq;
      int intents = docscore.get(doc).length;
      for (int i = 1; i < intents; i++) {

        double probdqi = docscore.get(doc)[i];
        double probdqs = 1.0;

        for (int docs : docset.keySet()){
          probdqs *= (1.0 - docset.get(docs)[i]);
        }
        score += ( lambda * prob_qiq * probdqi * probdqs);
      }
      if (score > max){
        max = score;
        maxdocid = doc;
      }
    }
    double[] score = docscore.get(maxdocid);
    docset.put(maxdocid, score);
    docscore.remove(maxdocid);
    r.add(maxdocid,max);
  }

  private static ScoreList getExpandedRanking(Map<String, String> parameters, String qid, PrintWriter expanded_out, String query, RetrievalModel model) throws IOException{

    ScoreList r = null;
    try {

      // check which reference system is to be used for getting documents to expand query
      if (parameters.containsKey("fbInitialRankingFile")) {

        // read a document ranking in trec_eval input format from the fbInitialRankingFile
        File parameterFile = new File(parameters.get("fbInitialRankingFile"));
        r = new ScoreList();
        Scanner scan = new Scanner(parameterFile);
        String line = null;
        String[] pair = null;
        for (int i = 0; i < Integer.parseInt(parameters.get("fbDocs")); i++) {

          if (!scan.hasNext()) {
            break;
          }
          line = scan.nextLine();
          pair = line.split(" ");

          while (scan.hasNext() && !pair[0].equals(qid)) {
            line = scan.nextLine();
            pair = line.split(" ");
          }

          if (!scan.hasNext()) {
            break;
          }

          r.add(Idx.getInternalDocid(pair[2]), Double.parseDouble(pair[4]));

        }
        scan.close();

      } else {
        r = processQuery(query, model);
      }

      HashMap<String, Double> q_learned = createLearnedQuery(r, parameters);
      double weight = Double.parseDouble(parameters.get("fbOrigWeight"));

      StringBuilder q_expanded = new StringBuilder("#WAND ( ");
      q_expanded.append(Double.toString(weight) + " " + model.defaultQrySopName() + " ( " + query + " ) " + Double.toString(1 - weight));
      StringBuilder q_learned_str = new StringBuilder(" #WAND ( ");

      for (String q : q_learned.keySet()) {
        String s = Double.toString(q_learned.get(q)) + " " + q + " ";
        q_learned_str.append(s);
      }
      q_learned_str.append(")");
      q_expanded.append(q_learned_str + " )");
      if (parameters.containsKey("fbExpansionQueryFile")) {
        expanded_out.println(qid + ": " + q_learned_str);
      }
      //System.out.println(q_expanded);
      r = processQuery(q_expanded.toString(), model);
//      return r;
    }catch (Exception ex) {
      ex.printStackTrace();
    }
    return  r;
  }

  /**
   *  This method will used the scorelist of the documents to find top fbTerms for creating learned query.
   *  @param : Scorelist r: contains list of documents with their corresponding score
   *  @param : parameters: Hashmap containing entries of the input file
   *  @return : hashmap containing top fbTerms with their corresponding score in sorted(decreasing) order
   */
  private static HashMap<String, Double> createLearnedQuery(ScoreList r, Map<String, String> parameters) throws IOException{

    HashMap<String, Double> map = new HashMap<String, Double>();

    // consider whichever is smaller between fbdocs and docs in scorelist
    // traverse for these docs and store all the terms apperaing in these docs in a map
    int loop = r.size() < Integer.parseInt(parameters.get("fbDocs")) ? r.size() : Integer.parseInt(parameters.get("fbDocs"));
    for (int i = 0; i < loop; i++){
      int doc_id = r.getDocid(i);
      TermVector obj = new TermVector(doc_id,"body");
      for (int j = 1; j < obj.stemsLength(); j++){
        String stem = obj.stemString(j);
        if (stem.contains(".") || stem.contains(",")){
          continue;
        }
        map.put(stem,0.0);
      }
    }
    // calculate the score for each of these terms using the Pseudo relevance feedback formula of Indri
    for(int i = 0; i < loop; i++){

      int doc_id = r.getDocid(i);
      TermVector obj = new TermVector(doc_id,"body");
      for (String stem : map.keySet()){
        int index = obj.indexOfStem(stem);
        int tf;
        if( index == -1){
          tf = 0;
        }else{
          tf = obj.stemFreq(index);
        }
        int len = Idx.getFieldLength("body", doc_id);
        float mu = Float.parseFloat(parameters.get("fbMu"));
        float probTermC = Idx.getTotalTermFreq("body", stem)/(float) Idx.getSumOfFieldLengths("body");
        float probTermDoc = (tf + mu * probTermC )/( len + mu );
        double probID = r.getDocidScore(i);
        double probTermI = probTermDoc * probID * Math.log(1/probTermC);

        map.put(stem, map.get(stem) + probTermI);
      }
    }
    // sort the hash map
    List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
      public int compare(Map.Entry<String, Double> o1,
                         Map.Entry<String, Double> o2) {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });

    map = new HashMap<String, Double>();
    //only store top m terms in the map
    int loop_var = Integer.parseInt(parameters.get("fbTerms")) < list.size() ? Integer.parseInt(parameters.get("fbTerms")) : list.size() ;
    for (int i = 0; i < loop_var; i++){

      map.put(list.get(i).getKey(), list.get(i).getValue());
    }

    return map;
  }

  /**
   *  Allocate the retrieval model and initialize it using parameters
   *  from the parameter file.
   *  @return The initialized retrieval model
   *  @throws IOException Error accessing the Lucene index.
   */
  private static RetrievalModel initializeRetrievalModel (Map<String, String> parameters)
    throws IOException {

    RetrievalModel model = null;
    String modelString = parameters.get ("retrievalAlgorithm").toLowerCase();

    if (modelString.equals("unrankedboolean")) {
      model = new RetrievalModelUnrankedBoolean();
    } else if (modelString.equals("rankedboolean")) {
      model = new RetrievalModelRankedBoolean();
    } else if (modelString.equals("bm25")) {

      float k_1 = Float.parseFloat(parameters.get("BM25:k_1"));
      float k_3 = Float.parseFloat(parameters.get("BM25:k_3"));
      float b = Float.parseFloat(parameters.get("BM25:b"));
      model = new RetrievalModelBM25(k_1, b, k_3);

    } else if (modelString.equals("indri")) {

      float mu =  Float.parseFloat(parameters.get("Indri:mu"));
      float lambda =  Float.parseFloat(parameters.get("Indri:lambda"));
      model = new RetrievalModelIndri(mu, lambda);

    } else if (modelString.equals("letor")){
      model = new RetrievalModelLETOR();
    }
    else {
      throw new IllegalArgumentException
        ("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }
      
    return model;
  }



  /**
   * Print a message indicating the amount of memory used. The caller can
   * indicate whether garbage collection should be performed, which slows the
   * program but reduces memory usage.
   * 
   * @param gc
   *          If true, run the garbage collector before reporting.
   */
  public static void printMemoryUsage(boolean gc) {

    Runtime runtime = Runtime.getRuntime();

    if (gc)
      runtime.gc();

    System.out.println("Memory used:  "
        + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
  }



  /**
   * Process one query.
   * @param qString A string that contains a query.
   * @param model The retrieval model determines how matching and scoring is done.
   * @return Search results
   * @throws IOException Error accessing the index
   */
  static ScoreList processQuery(String qString, RetrievalModel model)
    throws IOException {

    String defaultOp = model.defaultQrySopName ();
    qString = defaultOp + "(" + qString + ")";
    Qry q = QryParser.getQuery (qString);

    // Show the query that is evaluated
    
    //System.out.println("    --> " + q);
    
    if (q != null) {

      ScoreList r = new ScoreList ();
      
      if (q.args.size () > 0) {		// Ignore empty queries

        q.initialize (model);

        while (q.docIteratorHasMatch (model)) {
          int docid = q.docIteratorGetMatch ();
          double score = ((QrySop) q).getScore (model);
          r.add (docid, score);
          q.docIteratorAdvancePast (docid);
        }
      }

      r.sort();
      return r;
    } else
      return null;
  }



  /**
   *  Process the query file.
   *  @param queryFilePath
   *  @param model
   *  @throws IOException Error accessing the Lucene index.
   */
  static void processQueryFile(String queryFilePath,
                               RetrievalModel model)
      throws IOException {

    BufferedReader input = null;

    try {
      String qLine = null;

      input = new BufferedReader(new FileReader(queryFilePath));

      //  Each pass of the loop processes one query.

      while ((qLine = input.readLine()) != null) {
        int d = qLine.indexOf(':');

        if (d < 0) {
          throw new IllegalArgumentException
            ("Syntax error:  Missing ':' in query line.");
        }

        printMemoryUsage(false);

        String qid = qLine.substring(0, d);
        String query = qLine.substring(d + 1);

        System.out.println("Query " + qLine);

        ScoreList r = null;

        r = processQuery(query, model);

        if (r != null) {
          r.sort();
          printResults(qid, r);

        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      input.close();
    }
  }

  /**
   * Print the query results.
   * 
   * THIS IS NOT THE CORRECT OUTPUT FORMAT. YOU MUST CHANGE THIS METHOD SO
   * THAT IT OUTPUTS IN THE FORMAT SPECIFIED IN THE HOMEWORK PAGE, WHICH IS:
   * 
   * QueryID Q0 DocID Rank Score RunID
   * 
   * @param queryName
   *          Original query.
   * @param result
   *          A list of document ids and scores
   * @throws IOException Error accessing the Lucene index.
   */
  static void printResults(String queryName, ScoreList result) throws IOException {

    System.out.println(queryName + ":  ");
    if (result.size() < 1) {
      System.out.println("\tNo results.");
    } else {
      for (int i = 0; i < result.size(); i++) {
        System.out.println("\t" + i + ":  " + Idx.getExternalDocid(result.getDocid(i)) + ", "
            + result.getDocidScore(i));
      }
    }
  }

  /**
   *  Read the specified parameter file, and confirm that the required
   *  parameters are present.  The parameters are returned in a
   *  HashMap.  The caller (or its minions) are responsible for processing
   *  them.
   *  @return The parameters, in <key, value> format.
   */
  private static Map<String, String> readParameterFile (String parameterFileName)
    throws IOException {

    Map<String, String> parameters = new HashMap<String, String>();

    File parameterFile = new File (parameterFileName);

    if (! parameterFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + parameterFileName);
    }

    Scanner scan = new Scanner(parameterFile);
    String line = null;
    do {
      line = scan.nextLine();
      String[] pair = line.split ("=");
      parameters.put(pair[0].trim(), pair[1].trim());
    } while (scan.hasNext());

    scan.close();
    if ( !parameters.containsKey ("retrievalAlgorithm")){
      parameters.put("retrievalAlgorithm", "rankedboolean");
    }
    if (! (parameters.containsKey ("indexPath") &&
           parameters.containsKey ("queryFilePath") &&
           parameters.containsKey ("trecEvalOutputPath")
           )) {
      throw new IllegalArgumentException
        ("Required parameters were missing from the parameter file.");
    }

    return parameters;
  }

}
