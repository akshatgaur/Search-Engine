/**
 * Created by akshatgaur on 2/19/17.
 */

import java.io.*;
import java.util.*;

/**
 *  The NEAR operator for all retrieval models.
 */
public class QryIopNear extends QryIop {

    /**
     *  Evaluate the query operator; the result is an internal inverted
     *  list that may be accessed via the internal iterators.
     *  @throws IOException Error accessing the Lucene index.
     */
    private int operatorDistance;

    public QryIopNear(int operatorDistance){
        this.operatorDistance = operatorDistance;
    }


    protected void evaluate () throws IOException {

        //  Create an empty inverted list.  If there are no query arguments,
        //  that's the final result.

        this.invertedList = new InvList (this.getField());

        if (args.size () == 0) {
            return;
        }

        //  Each pass of the loop adds 1 document to result inverted list
        //  until all of the argument inverted lists are depleted.

        boolean documentLeft = true;
        while ( documentLeft) {

            //First get the doc in which all the arguments are present.
            // Once the doc is available get the location of each argument and check if it satisfies the near operator.

            boolean notAllInDoc = false;
            Qry q_0 = this.args.get(0);
            if(!((QryIop)q_0).docIteratorHasMatch(null)){
                notAllInDoc = true;
                break;
            }
            int maxDocid = q_0.docIteratorGetMatch();
            for (int i = 1; i < this.args.size(); i++) {
                Qry q_i = args.get(i);

                if (q_i.docIteratorHasMatch(null)) {
                    int q_iDocid = q_i.docIteratorGetMatch();

                    if (maxDocid != q_iDocid) {
                        notAllInDoc = true;
                        if (maxDocid < q_iDocid) {
                            maxDocid = q_iDocid;
                        }
                    }

                } else{
                    notAllInDoc = true;
                    break;
                }
            }

            //If all arguments are not in the doc then advance ahead of the max value of docID of that argument we have
            // as there will be no matching doc ID before the that doc. Continue looking for all docs in next iteration.

            if (notAllInDoc) {
                for (Qry q_i : this.args) {
                    if (q_i.docIteratorHasMatch(null)){
                        q_i.docIteratorAdvanceTo(maxDocid);
                    }else{
                        documentLeft = false;
                    }
                }
                continue;
            }

            //  Create a new posting by storing the position that satisfies near operator constraint for the arguments.

            List<Integer> position = new ArrayList<Integer>();
            while (true) {
                boolean locRemaining = true;

                Qry q = this.args.get(0);
                if (!((QryIop) q).locIteratorHasMatch()){
                    locRemaining = false;
                    break;
                }

                // get the location of curr argument and next argument and check if it satisfies
                //if it does not advance the argument with smaller loc to next location and start iterating back from the beginning.

                int loc = 0;
                int curr_loc = 0;
                int next_loc = 0;
                for (int i = 0; i < this.args.size() - 1; i++) {
                    QryIop q_curr = (QryIop) this.args.get(i);
                    QryIop q_next = (QryIop) this.args.get(i+1);

                    if (q_curr.locIteratorHasMatch()){
                        curr_loc = ((QryIop) q_curr).locIteratorGetMatch();
                    } else{
                        locRemaining = false;
                        break;
                    }
                    if (q_next.locIteratorHasMatch()){
                        next_loc = ((QryIop) q_next).locIteratorGetMatch();
                    } else{
                        locRemaining = false;
                        break;
                    }
                    if (next_loc - curr_loc > operatorDistance ){
                        if (q_curr.locIteratorHasMatch()){
                            q_curr.locIteratorAdvance();
                            i = -1;
                            continue;
                        } else {
                            locRemaining = false;
                            break;
                        }
                    } if (next_loc - curr_loc < 0){
                        if (q_next.locIteratorHasMatch()){
                            q_next.locIteratorAdvance();
                            i = -1;
                            continue;
                        } else {
                            locRemaining = false;
                            break;
                        }
                    }
                    loc = next_loc;
                }
                //if there is no location remaining/matching then break and get the next document
                if (!locRemaining) {
                    break;
                }
                //if there is a set of location satisfying near operator then add to position arraylist
                position.add(loc);
                for (Qry query : this.args){
                    ((QryIop) query).locIteratorAdvance();
                }

            }
            // if there is are entries in position then append that to the inverted list.
            if (position.size() > 0){
                this.invertedList.appendPosting(maxDocid, position);
            }

            //once a document is checked advance all the arguments past that document.
            for (Qry queries : this.args) {
                queries.docIteratorAdvancePast(maxDocid);
            }
        }
    }
}