/**
 * Created by akshatgaur on 2/18/17.
 */

/**
 *  An object that stores parameters for the BM25
 *  retrieval model (k_1=1.2, b=0.75, k_3=0.) and indicates to the query
 *  operators how the query should be evaluated.
 */

public class RetrievalModelIndri extends RetrievalModel {

    private float mu, lambda;

    RetrievalModelIndri(float mu, float lambda){
        this.mu = mu;
        this.lambda = lambda;
    }

    public float getMu(){ return this.mu; }

    public float getLambda(){ return this.lambda; }

    public String defaultQrySopName () {
        return new String ("#and");
    }

}

