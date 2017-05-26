/**
 * Created by akshatgaur on 2/18/17.
 */

/**
 *  An object that stores parameters for the BM25
 *  retrieval model (k_1=1.2, b=0.75, k_3=0.) and indicates to the query
 *  operators how the query should be evaluated.
 */

public class RetrievalModelBM25 extends RetrievalModel {

    private float k_1, b , k_3;

    RetrievalModelBM25(float k_1, float b, float k_3){
        this.k_1 = k_1;
        this.k_3 = k_3;
        this.b = b;

    }

    public float getK_1(){ return this.k_1; }

    public float getB(){ return this.b; }

    public float getK_3(){return this.k_3; }

    public String defaultQrySopName () {
        return new String ("#sum");
    }

}

