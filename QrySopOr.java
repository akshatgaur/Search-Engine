/**
 *  Copyright (c) 2017, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopOr extends QrySop {

  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchMin (r);
  }

  /**
   *  Get a score for the document that docIteratorHasMatch matched.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScore (RetrievalModel r) throws IOException {


    // add line for Ranked Boolean

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean (r);
    } else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean (r);
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the OR operator.");
    }
  }
  
  /**
   *  getScore for the UnrankedBoolean retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  private double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {

      int doc_id = Qry.INVALID_DOCID;
      if (this.docIteratorHasMatch(null)){
        doc_id = this.docIteratorGetMatch();
      }
      double max_score =  0.0;

      // Calculate the score by getting the max out of each arguments frequency.
      // Check if the argument's document frequency is same as the current document frequency.

      for(int i = 0; i < this.args.size(); i++){
        Qry q_i =  this.args.get(i);

        //check if the arguments have same docID
        if ( !q_i.docIteratorHasMatchCache() || doc_id != q_i.docIteratorGetMatch()){
          continue;
        }
        double score =  ((QrySop) q_i).getScore(r);
        if (max_score < score){
          max_score = score;
        }
      }
      return max_score;
    }
  }

  public double getDefaultScore (RetrievalModel r, int doc_id) throws IOException{
      return 0.0;
  }
}
