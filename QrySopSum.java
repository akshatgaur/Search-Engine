import java.io.*;

/**
 * Created by akshatgaur on 2/18/17.
 */


/**
 *  The SUM operator for all retrieval models.
 */

public class QrySopSum extends QrySop {

    /**
     *  Indicates whether the query has a match.
     *  @param r The retrieval model that determines what is a match
     *  @return True if the query matches, otherwise false.
     */
    public boolean docIteratorHasMatch (RetrievalModel r) {

        // Match the document that has all the arguments present in it.
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

        if (r instanceof RetrievalModelBM25) {
            return this.getScoreBM25 (r);
        } else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the SUM operator.");
        }
    }

    /**
     *  Get a score for the BM25 model if term is present in the doc ID.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    private double getScoreBM25 (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {

            //To Calculate the score for BM25 get the tf,idf and RSJ weight for each term
            // multiply it with user query term weight
            // sum this score for each query term.
            int doc_id = this.docIteratorGetMatch();
            double score =  0.0;
            for (Qry q_i: this.args  ) {
                if ( !q_i.docIteratorHasMatchCache() || doc_id != q_i.docIteratorGetMatch()){
                    continue;
                }
                double term_score =  ((QrySop) q_i).getScore(r);
                int qtf = 1;
                double qtf_score =  (((RetrievalModelBM25) r).getK_3() + 1) * qtf / (((RetrievalModelBM25) r).getK_3() + qtf);
                score += ( term_score * qtf_score );

            }
            return score;
        }
    }

    /**
     *  Get a score for the BM25 model if term is not present in the doc ID.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    public double getDefaultScore (RetrievalModel r, int doc_id) throws IOException{
        return 0.0;
    }
}
