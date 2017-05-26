
import java.io.*;
import java.util.*;

/**
 * Created by akshatgaur on 4/2/17.
 */
public class LearningToRank {

    private Map<String, String> parameters ;
    HashMap<String,Double> pgRank;

    public LearningToRank(Map<String, String> parameters, boolean pgRank) throws IOException {

        this.parameters = parameters;
        //get page rank
        if (pgRank){
            this.pgRank = getPageRank();
        }
    }

    public void fit_transform() throws IOException {

        // get feature list
        double[] min = new double[18];
        double[] max = new double[18];
        Arrays.fill(min, Double.MAX_VALUE);
        Arrays.fill(max, -Double.MAX_VALUE);
        BufferedReader queries = null;
        try {
            String qLine = null;
            queries = new BufferedReader(new FileReader(parameters.get("letor:trainingQueryFile")));

            // get each query for training
            BufferedReader train_docs;// = new BufferedReader(new FileReader(parameters.get("letor:trainingQrelsFile")));
            HashMap<String, double[]> qd_feat_list;
            PrintWriter out = new PrintWriter(parameters.get("letor:trainingFeatureVectorsFile" ));

            while ((qLine = queries.readLine()) != null) {
                qd_feat_list = new HashMap<String, double[]>();
                Arrays.fill(min, Double.MAX_VALUE);
                Arrays.fill(max, -Double.MAX_VALUE);

                int d = qLine.indexOf(':');
                if (d < 0) {
                    throw new IllegalArgumentException
                            ("Syntax error:  Missing ':' in query line.");
                }
                train_docs = new BufferedReader(new FileReader(parameters.get("letor:trainingQrelsFile")));
                String qid = qLine.substring(0, d);
                String query = qLine.substring(d + 1);
                String[] qTerms = QryParser.tokenizeString(query);
                String docLine = null;
                String[] pair = null;

                while ((docLine = train_docs.readLine()) != null) {
                    pair = docLine.split(" ");
                    if (pair[0].equals(qid)) {
                        //push in hashmap
                        String key = pair[2] + ":" + pair[3];
                        double[] feat_list = getFeatureList(qTerms, pair[2], min, max);
                        if(feat_list != null){
                            qd_feat_list.put(key, feat_list) ;
                        }
                    }
                }
                //normalize and write to file
                normalizeAndWrite(out, qid, qd_feat_list, max, min );
            }
            out.close();

            //train model using SVM
            Process cmdProc = Runtime.getRuntime().exec(
                    new String[] { parameters.get("letor:svmRankLearnPath"), "-c", parameters.get("letor:svmRankParamC"), parameters.get("letor:trainingFeatureVectorsFile"),
                            parameters.get("letor:svmRankModelFile") });
            SVM(cmdProc);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            queries.close();
        }
    }

    public void transform() throws IOException {

        // get feature list
        double[] min = new double[18];
        double[] max = new double[18];

        BufferedReader queries = null;
        int qnum = 0;
        try {
            String qLine = null;
            queries = new BufferedReader(new FileReader(parameters.get("queryFilePath")));

            // get each query for test
            HashMap<String, double[]> qd_feat_list;
            PrintWriter out = new PrintWriter(parameters.get("letor:testingFeatureVectorsFile" ));
            PrintWriter result = new PrintWriter(parameters.get("trecEvalOutputPath" ));
            ScoreList r = null;

            while ((qLine = queries.readLine()) != null) {
                qd_feat_list = new HashMap<String, double[]>();
                Arrays.fill(min, Double.MAX_VALUE);
                Arrays.fill(max, -Double.MAX_VALUE);
                qnum++;
                int d = qLine.indexOf(':');
                if (d < 0) {
                    throw new IllegalArgumentException
                            ("Syntax error:  Missing ':' in query line.");
                }

                String qid = qLine.substring(0, d);
                String query = qLine.substring(d + 1);
                String[] qTerms = QryParser.tokenizeString(query);

                //get top 100 docs for test using BM25
                float k_1 = Float.parseFloat(parameters.get("BM25:k_1"));
                float k_3 = Float.parseFloat(parameters.get("BM25:k_3"));
                float b = Float.parseFloat(parameters.get("BM25:b"));
                RetrievalModel model = new RetrievalModelBM25(k_1, b, k_3);
                r = QryEval.processQuery(query, model);

                if (r != null) {
                    r.sort();

                    int max_size = 100;
                    int loop = max_size < r.size() ? max_size : r.size();
                    for (int i = 0; i < loop; i++) {
                        //push in hashmap
                            String externalDocID = Idx.getExternalDocid(r.getDocid(i));
                            String key = externalDocID + ":" + 0;
                            double[] feat_list = getFeatureList(qTerms, externalDocID, min, max);
                            if(feat_list != null){
                                qd_feat_list.put(key, feat_list) ;
                            }
                    }
                    //normalize and write to file
                    normalizeAndWrite(out, qid, qd_feat_list, max, min );
//                    out.close();
                }

            }
            out.close();

            //test model using SVM
            Process cmdProc = Runtime.getRuntime().exec(
                    new String[] { parameters.get("letor:svmRankClassifyPath"), parameters.get("letor:testingFeatureVectorsFile"),
                            parameters.get("letor:svmRankModelFile"), parameters.get("letor:testingDocumentScores") });
            SVM(cmdProc);

            //write the results into output file
            BufferedReader test_feat_vector = new BufferedReader(new FileReader(parameters.get("letor:testingFeatureVectorsFile")));
            BufferedReader test_doc_score = new BufferedReader(new FileReader(parameters.get("letor:testingDocumentScores")));
            ScoreList[] rnew = new ScoreList[qnum];
            String featLine = null;
            String docLine = null;
            HashSet<Integer> qids = new HashSet<Integer>();
            int idx = -1;
            int qid = -1;

            while ((featLine = test_feat_vector.readLine()) != null && (docLine = test_doc_score.readLine()) != null) {
                String[] splitline = featLine.split(" ");
                int prevq = qid;
                qid = Integer.parseInt(splitline[1].split(":")[1]);
                while (!qids.contains(qid)) {

                    // store result of previous qid into file
                    if (idx != -1) {
                        rnew[idx].sort();
                        int max_size = 100;
                        int loop = max_size < rnew[idx].size() ? max_size : rnew[idx].size();
                        for (int i = 0; i < loop; i++) {
                            result.print(prevq + " Q0 " + Idx.getExternalDocid(rnew[idx].getDocid(i)) + " " + (i + 1) + " " + rnew[idx].getDocidScore(i) + " agaur\n");
                        }
                    }
                    // add new qid
                    qids.add(qid);
                    idx++;
                    rnew[idx] = new ScoreList();
                }
                String external_doc_id = splitline[splitline.length - 1];
                rnew[idx].add(Idx.getInternalDocid(external_doc_id), Double.parseDouble(docLine));
            }
            rnew[idx].sort();
            int max_size = 100;
            int loop = max_size < rnew[idx].size() ? max_size : rnew[idx].size();
            for (int i = 0; i < loop; i++) {
                result.print(qid + " Q0 " + Idx.getExternalDocid(rnew[idx].getDocid(i)) + " " + (i + 1) + " " + rnew[idx].getDocidScore(i) + " agaur\n");
            }
            result.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            queries.close();

        }
    }

    public double[] getFeatureList(String[] qTerms, String doc_id, double[] min, double[] max) throws IOException {

        int internalDocID;
        double[] feat_list = new double[18];
        try {
            internalDocID = Idx.getInternalDocid(doc_id);
        } catch (Exception e) {
            return null;
        }
        int index = 0;

        //get all features

        //spam score
        feat_list[0] = Double.parseDouble(Idx.getAttribute("score", internalDocID));
        getMinMax(feat_list[index], max, min, index++);

        // url depth
        String rawUrl = Idx.getAttribute("rawUrl", internalDocID);
        double depth = rawUrl.length() - rawUrl.replace("/", "").length();
        feat_list[1] = depth;
        getMinMax(feat_list[index], max, min, index++);

        // from wikipedia score
        feat_list[2] = rawUrl.contains("wikipedia.org") ? 1.0 : 0.0;
        getMinMax(feat_list[index], max, min, index++);

        // page rank score
        if (pgRank.containsKey(doc_id)){
            feat_list[3] = pgRank.get(doc_id);
            getMinMax(feat_list[index], max, min, index++);
        }else{
            feat_list[3] = Double.NaN;
        }

        // feat-5 BM25 for body
        feat_list[4] = getScoreBM25(internalDocID, "body", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-6 Indri body
        feat_list[5] = getScoreIndri(internalDocID, "body", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-7 Term overlap body
        feat_list[6] = termOverlapScore(internalDocID, "body", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-8 BM25 for title
        feat_list[7] = getScoreBM25(internalDocID, "title", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-9 Indri Title
        feat_list[8] = getScoreIndri(internalDocID, "title", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-10 Term overlap title
        feat_list[9] = termOverlapScore(internalDocID, "title", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-11 BM25 for url
        feat_list[10] = getScoreBM25(internalDocID, "url", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-12 Indri Url
        feat_list[11] = getScoreIndri(internalDocID, "url", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-13 Term overlap url
        feat_list[12] = termOverlapScore(internalDocID, "url", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-14 BM25 for inlink
        feat_list[13] = getScoreBM25(internalDocID, "inlink", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-15 Indri inlink
        feat_list[14] = getScoreIndri(internalDocID, "inlink", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        //feat-16 Term overlap inlink
        feat_list[15] = termOverlapScore(internalDocID, "inlink", qTerms);
        getMinMax(feat_list[index], max, min, index++);

        // Custom Features
        // Rankedboolean AND
        feat_list[16] = getScoreRankedBooleanAnd(internalDocID, qTerms, "body");
        getMinMax(feat_list[index], max, min, index++);

        //feat-7 Term overlap body
        feat_list[17] = getScoreRankedBooleanOR(internalDocID, qTerms, "url");
        getMinMax(feat_list[index], max, min, index);

        // q-idf score
//        feat_list[17] = getQidf(qTerms);
//        getMinMax(feat_list[index], max, min, index);


//        feat_list[17] = qTerms.length;
//        getMinMax(feat_list[index], max, min, index);

//        feat_list[17] = qTerms.length;
//        getMinMax(feat_list[index], max, min, index++);

        return feat_list;
    }

    public double getQidf(String[] qTerms) throws IOException{

        double score = 0.0;
        long corpus_freq = Idx.getSumOfFieldLengths("body");
        for (String q : qTerms){
            long tf = Idx.getTotalTermFreq("body", q);

            score += ((double) tf)/corpus_freq;
        }
        return score;
    }

    public void normalizeAndWrite(PrintWriter out, String qid, HashMap<String, double[]> qd_feat_list, double[] max, double[] min ){

            for (String key : qd_feat_list.keySet()) {
                String[] val = key.split(":");
                out.print(val[1] + " qid:" + qid + " ");
                double[] feat = qd_feat_list.get(key);
                String[] disabledFeatures = null;
                if (parameters.containsKey("letor:featureDisable")) {
                    disabledFeatures = parameters.get("letor:featureDisable").split(",");
                }
                for (int i = 0; i < feat.length; i++) {
                    if (disabledFeatures!= null && Arrays.asList(disabledFeatures).contains(""+(i+1)) ){
                        continue;
                    }
                    if (Double.isNaN(feat[i])){
                        feat[i] = 0.0;
                    }else{
                        if (max[i] == min[i]){
                            feat[i] = 0.0;
                        }else{
                            feat[i] = (feat[i] - min[i]) / (max[i] - min[i]);
                        }
                    }
                    out.print((i + 1) + ":" + feat[i] + " ");
                }
                out.print("# " + val[0] + "\n");
            }
    }

    public HashMap<String,Double> getPageRank() throws IOException {

        // get page rank from the file
        String line = null;
        BufferedReader pagerank = new BufferedReader(new FileReader(parameters.get("letor:pageRankFile")));
        HashMap<String,Double> pgRank = new HashMap<String, Double>();
        String[] split;
        while((line = pagerank.readLine()) != null){
            split = line.split("\t");
            pgRank.put(split[0], (double)(Float.parseFloat(split[1])));
        }
        return pgRank;
    }

    public double getScoreBM25 (int doc_id, String field, String[] q) throws IOException {

//        int tf = ((QryIop) q).docIteratorGetMatchPosting().tf;
//        int df = ((QryIop) q).getDf()
//            float k1 = ((RetrievalModelBM25) r).getK_1();
//            float b = ((RetrievalModelBM25) r).getB();
        TermVector termVectorObj = new TermVector(doc_id, field);
        if (termVectorObj.stemsLength() == 0){
            return Double.NaN;
        }
        double score = 0.0;
        for (String q_i : q){

            int idx = termVectorObj.indexOfStem(q_i);
            if (idx == -1){
               continue;
            }

            int tf = termVectorObj.stemFreq(idx);
            int df = termVectorObj.stemDf(idx);
            double N = (double)Idx.getNumDocs();
            float k1 = Float.parseFloat(parameters.get("BM25:k_1"));
            float b = Float.parseFloat(parameters.get("BM25:b"));
            long doc_len = Idx.getFieldLength(field, doc_id);

            double RSJ_wt = Math.max(0, Math.log( (N - df + 0.5) / (df + 0.5) ));
            double avg_doc_len = (double)Idx.getSumOfFieldLengths(field) / Idx.getDocCount(field);
            double term_wt = tf / (tf + k1 * ( 1 - b + ( b * doc_len / avg_doc_len)));

            score += RSJ_wt * term_wt;
        }
        return score;
    }

    private double getScoreRankedBooleanAnd (int docID, String[] qTerms, String field) throws IOException {

        double score = Double.MAX_VALUE;
        TermVector obj = new TermVector(docID, field);
        for (String q : qTerms) {
            if (obj.stemsLength() == 0){
                return Double.NaN;
            }
            int idx = obj.indexOfStem(q);
            if ( idx == -1){
                return 0;
            }
            int tf = obj.stemFreq(idx);
            score = tf < score ? tf : score;
        }
        return score;
    }

    private double getScoreRankedBooleanOR (int docID, String[] qTerms, String field) throws IOException {

        double score = 0.0;
        TermVector obj = new TermVector(docID, field);
        for (String q : qTerms) {
            if (obj.stemsLength() == 0){
                return Double.NaN;
            }
            int idx = obj.indexOfStem(q);
            if ( idx == -1){
                continue;
            }
            int tf = obj.stemFreq(idx);
            score = tf > score ? tf : score;
        }
        return score;
    }

    public double getScoreIndri (int doc_id, String field, String[] q) throws IOException {

        TermVector termVectorObj = new TermVector(doc_id, field);
        if (termVectorObj.stemsLength() == 0){
            return Double.NaN;
        }
        double score = 1.0;
        int queryTermsMissing = 0;
        for (String q_i : q){
            float lambda = Float.parseFloat(parameters.get("Indri:lambda"));
            float mu = Float.parseFloat(parameters.get("Indri:mu"));
            int idx = termVectorObj.indexOfStem(q_i);
            int tf = 0;
            queryTermsMissing++;
            if (idx != -1){
                tf = termVectorObj.stemFreq(idx);
                queryTermsMissing--;
            }

            double ctf = Idx.getTotalTermFreq(field, q_i);
            double prob_mle_C = ctf / Idx.getSumOfFieldLengths(field);
            double prob_q = (1 - lambda) * ( (tf + mu * prob_mle_C) / ( Idx.getFieldLength(field, doc_id) + mu) ) + lambda * prob_mle_C;
            score *= prob_q;

        }
        if (queryTermsMissing == q.length){
            return 0;
        }

        return Math.pow(score, 1/(double)q.length);

    }

    public double termOverlapScore(int doc_id, String field, String[] q) throws IOException {
        double count = 0.0;
        TermVector obj = new TermVector(doc_id, field);
        if (obj.stemsLength() == 0){
            return Double.NaN;
        }
        for (String q_i : q){
            try{
                if (obj.indexOfStem(q_i) != -1)
                    count++;
            }catch (Exception e){
                System.out.println(e);
            }

        }

        return (count/q.length ) ;

    }

    public void getMinMax(double feat, double[] max, double[] min, int index){

        if (Double.isNaN(feat))
            return;
        if (feat > max[index]){
            max[index] = feat;
        }else if (feat < min[index]){
            min[index] = feat;
        }
    }

    public void SVM(Process cmdProc) throws Exception {
        // runs svm_rank_learn from within Java to train the model
        // execPath is the location of the svm_rank_learn utility,
        // which is specified by letor:svmRankLearnPath in the parameter file.
        // FEAT_GEN.c is the value of the letor:c parameter.

        // The stdout/stderr consuming code MUST be included.
        // It prevents the OS from running out of output buffer space and stalling.

        // consume stdout and print it out for debugging purposes
        BufferedReader stdoutReader = new BufferedReader(
                new InputStreamReader(cmdProc.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
//            System.out.println(line);
        }
        // consume stderr and print it for debugging purposes
        BufferedReader stderrReader = new BufferedReader(
                new InputStreamReader(cmdProc.getErrorStream()));
        while ((line = stderrReader.readLine()) != null) {
//            System.out.println(line);
        }

        // get the return value from the executable. 0 means success, non-zero
        // indicates a problem
        int retValue = cmdProc.waitFor();
        if (retValue != 0) {
            throw new Exception("SVM Rank crashed.");
        }
    }
}
