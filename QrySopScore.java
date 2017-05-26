/**
 *  Copyright (c) 2017, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;

/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

  /**
   *  Document-independent values that should be determined just once.
   *  Some retrieval models have these, some don't.
   */
  
  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchFirst (r);
  }

  /**
   *  Get a score for the document that docIteratorHasMatch matched.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScore (RetrievalModel r) throws IOException {

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean (r);
    }else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean (r);
    } else if (r instanceof RetrievalModelBM25) {
      return this.getScoreBM25 (r);
    } else if (r instanceof RetrievalModelIndri) {
      return this.getScoreIndri (r);
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }

  /**
   *  getScore for the BM25 retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreBM25 (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {

      Qry q = this.args.get(0);
      String field = ((QryIop) q).field;
      int doc_id = ((QryIop) q).docIteratorGetMatch();
      int tf = ((QryIop) q).docIteratorGetMatchPosting().tf;
      int df = ((QryIop) q).getDf();
      double N = (double)Idx.getNumDocs();
      float k1 = ((RetrievalModelBM25) r).getK_1();
      float b = ((RetrievalModelBM25) r).getB();
      long doc_len = Idx.getFieldLength(field, doc_id);

      double RSJ_wt = Math.max(0, Math.log( (N - df + 0.5) / (df + 0.5) ));
      double avg_doc_len = (double)Idx.getSumOfFieldLengths(field) / Idx.getDocCount(field);
      double term_wt = tf / (tf + k1 * ( 1 - b + ( b * doc_len / avg_doc_len)));

      return RSJ_wt * term_wt;
    }
  }

  //Calculate score for Indri when the term is present in the doc using the formula.

  public double getScoreIndri (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      //calculate probability

      Qry q = this.args.get(0);
      String field = ((QryIop)q).field;
      int doc_id = ((QryIop)q).docIteratorGetMatch();
      float lambda = ((RetrievalModelIndri)r).getLambda();
      float mu = ((RetrievalModelIndri)r).getMu();
      int tf = ((QryIop) q).docIteratorGetMatchPosting().tf;
      double ctf = ((QryIop) q).getCtf();
      double prob_mle_C = ctf / Idx.getSumOfFieldLengths(field);
      double prob_q = (1 - lambda) * ( (tf + mu * prob_mle_C) / ( Idx.getFieldLength(field, doc_id) + mu) ) + lambda * prob_mle_C;

      return prob_q;

    }
  }

  //Calculate score for Indri when the term is present in the doc using the formula.

  public double getDefaultScoreIndri (RetrievalModel r, int doc_id) throws IOException {

      //calculate score if matched by getting the arguments frequency

      Qry q = this.args.get(0);
      String field = ((QryIop)q).field;
      float lambda = ((RetrievalModelIndri)r).getLambda();
      float mu = ((RetrievalModelIndri)r).getMu();
      int tf = 0;
      double ctf = ((QryIop) q).getCtf();
      double prob_mle_C = ctf / Idx.getSumOfFieldLengths(field);
      double prob_q = (1 - lambda) * ( (tf + mu * prob_mle_C) / ( Idx.getFieldLength(field, doc_id) + mu) ) + lambda * prob_mle_C;

      return prob_q;
  }


  public double getDefaultScore (RetrievalModel r, int doc_id) throws IOException {

    if (r instanceof RetrievalModelIndri) {
      return this.getDefaultScoreIndri (r, doc_id);
    } else {
      throw new IllegalArgumentException
              (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }

  
  /**
   *  getScore for the Unranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }

  /**
   *  getScore for the Ranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      //calculate score if matched by getting the arguments frequency

      Qry q = this.args.get(0);
      return ((QryIop) q).docIteratorGetMatchPosting().tf;

    }
  }

  /**
   *  Initialize the query operator (and its arguments), including any
   *  internal iterators.  If the query operator is of type QryIop, it
   *  is fully evaluated, and the results are stored in an internal
   *  inverted list that may be accessed via the internal iterator.
   *  @param r A retrieval model that guides initialization
   *  @throws IOException Error accessing the Lucene index.
   */
  public void initialize (RetrievalModel r) throws IOException {

    Qry q = this.args.get (0);
    q.initialize (r);
  }


  //override the getweight function of Qry
  //This method will be called by QrySopXXX classes to get the weights for that particular term.
 // @Override
//  public float getWeight() {
//
//    return this.args.get(0).getWeight();
//  }
}
