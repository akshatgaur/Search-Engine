
/**
 * Created by akshatgaur on 2/19/17.
 */
import java.io.*;


/**
 *  The WAND operator for all retrieval models.
 */
public class QrySopWSum extends QrySop {

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

        if (r instanceof RetrievalModelIndri) {
            return this.getScoreIndri (r);
        } else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the AND operator.");
        }
    }


    //calculate score for Indri Retrieval Model
    private double getScoreIndri (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {

            int doc_id = Qry.INVALID_DOCID;
            if (this.docIteratorHasMatch(r)){
                doc_id = this.docIteratorGetMatch();
            }
            double score =  0.0;
            double total_wt = 0.0;

            for(int i = 0; i < this.args.size(); i++){
                Qry q_i =  this.args.get(i);
                double curr_score;
                float weight = ((QrySop) q_i).getWeight();

                //check if the arguments have same docID
                // call getdefaultScore method if the term is not there in current docID or if there is no match left for the term
                if ( !q_i.docIteratorHasMatchCache() || doc_id != q_i.docIteratorGetMatch()){
                    curr_score = ((QrySop) q_i).getDefaultScore(r, doc_id);
                    curr_score = (curr_score * ((QrySop) q_i).getWeight());
                    score += curr_score;
                    total_wt += ((QrySop) q_i).getWeight();
                } else{
                    curr_score = ((QrySop) q_i).getScore(r);
                    curr_score = (curr_score * ((QrySop) q_i).getWeight());
                    score += curr_score;
                    total_wt += ((QrySop) q_i).getWeight();
                }
            }
            return (score / total_wt);
        }
    }

    public double getDefaultScore (RetrievalModel r, int doc_id) throws IOException{

        double score = 0.0;
        double total_wt = 0.0;
        if (r instanceof RetrievalModelIndri) {
            double curr_score;
            for ( Qry q_i : this.args ) {
                float weight = ((QrySop) q_i).getWeight();
                curr_score = ((QrySop) q_i).getDefaultScore(r, doc_id);
                curr_score = (curr_score * ((QrySop) q_i).getWeight());
                score += curr_score;
                total_wt += ((QrySop) q_i).getWeight();
            }
        }
        return (score / total_wt);
    }
}
