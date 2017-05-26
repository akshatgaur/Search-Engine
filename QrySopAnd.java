/**
 * Created by akshatgaur on 2/19/17.
 */

import java.io.*;

/**
 *  The AND operator for all retrieval models.
 */
public class QrySopAnd extends QrySop {

    /**
     *  Indicates whether the query has a match.
     *  @param r The retrieval model that determines what is a match
     *  @return True if the query matches, otherwise false.
     */
    public boolean docIteratorHasMatch (RetrievalModel r) {

        // For Indri Model match the document that has any of the arguments present in it.
        if (r instanceof RetrievalModelIndri) {
            return this.docIteratorHasMatchMin(r);
        }
        // Match the document that has all the arguments present in it.
        return this.docIteratorHasMatchAll (r);

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
        } else if (r instanceof RetrievalModelIndri) {
            return this.getScoreIndri (r);
        } else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the AND operator.");
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

            //To Calculate the score get the minimum value which is the minimum value out of all the argument frequencies.

            Qry q_0 = this.args.get(0);
            double min_score =  ((QrySop) q_0).getScore(r);
            for(int i = 1; i < this.args.size(); i++){

                Qry q_i =  this.args.get(i);
                double score =  ((QrySop) q_i).getScore(r);
                if (min_score > score){
                    min_score = score;
                }
            }

            return min_score;
        }
    }

    /**
     *  getScore for the Indri retrieval model.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    private double getScoreIndri (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {

            int doc_id = Qry.INVALID_DOCID;
            if (this.docIteratorHasMatch(r)){
                doc_id = this.docIteratorGetMatch();
            }
            double score =  1.0;

            for(int i = 0; i < this.args.size(); i++){
                Qry q_i =  this.args.get(i);

                //check if the arguments have same docID
                if ( !q_i.docIteratorHasMatchCache() || doc_id != q_i.docIteratorGetMatch()){
                    score *= ((QrySop) q_i).getDefaultScore(r, doc_id);
                } else{
                    score *= ((QrySop) q_i).getScore(r);
                }
            }
            return Math.pow(score, 1.0 / this.args.size());
        }
    }

    public double getDefaultScore (RetrievalModel r, int doc_id) throws IOException{

        double score = 1.0;
        if (r instanceof RetrievalModelIndri) {

            for ( Qry q_i : this.args ) {

                score *= ((QrySop) q_i).getDefaultScore(r, doc_id);
            }
        }
        return Math.pow(score, 1.0 / this.args.size());
    }
}
