package cn.fudan.cs.stree.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class ManualQueryAutomata<L> extends Automata<L> {

    private HashMap<Integer, HashMap<L, Integer>> transitions;

    // overwrite the private field in super class
    private int numOfStates;
    private final Set<L> initialLabelSet = new HashSet<>();

    public static ManualQueryAutomata<String> getManualQuery(String queryCase){
        ManualQueryAutomata<String> query = null;
        switch (queryCase){
            // a* b? c
            case "q_0":
                query = new ManualQueryAutomata<>(4);
                query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "c", 3);
                query.addTransition(1, "c", 3);
                break;
            case "q_1":
                // a (b c)*
                query = new ManualQueryAutomata<String>(4);
                query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(1, "c", 3);
                query.addTransition(2, "b", 2);
                query.addTransition(2, "c", 3);
                break;
            case "q_2":
                // a* b*
                query = new ManualQueryAutomata<>(3);
                query.addFinalState(1);query.addFinalState(2);
                query.addTransition(0, "a", 1);
                query.addTransition(0, "b", 2);
                query.addTransition(1, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "b", 2);
                break;
            case "q_3":
                // a b*
                query = new ManualQueryAutomata<String>(3);
                query.addFinalState(1);query.addFinalState(2);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "b", 2);
                break;
            case "q_4":/// update
                // a* | b*
                query = new ManualQueryAutomata<String>(3);
                query.addFinalState(1);query.addFinalState(2);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "a", 1);
                query.addTransition(0, "b", 2);
                query.addTransition(2, "b", 2);
                break;
            case "q_5":/// update
                // a* b* c*
                query = new ManualQueryAutomata<String>(4);
                query.addFinalState(1);query.addFinalState(2);query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(1, "a", 1);

                query.addTransition(0, "b", 2);
                query.addTransition(1, "b", 2);
                query.addTransition(2, "b", 2);

                query.addTransition(0, "c", 3);
                query.addTransition(1, "c", 3);
                query.addTransition(2, "c", 3);
                query.addTransition(3, "c", 3);
                break;
            case "q_6":/// update
                // a b* | c
                query = new ManualQueryAutomata<String>(3);
                query.addFinalState(2);query.addFinalState(3);
                query.addTransition(0, "a", 1);
                query.addTransition(0, "b", 2);
                query.addTransition(2, "b", 2);
                query.addTransition(1, "c", 3);
                break;
            case "q_7":
                query = new ManualQueryAutomata<String>(4);
                query.addFinalState(3);
                // a b* c*
                query.addTransition(0, "a", 1);
                query.addTransition(1, "b", 2);
                query.addTransition(1, "c", 3);
                query.addTransition(2, "b", 2);
                query.addTransition(2, "c", 3);
                query.addTransition(3, "c", 3);
                break;
            default:
                break;
        }
        return query;
    }

    public ManualQueryAutomata(int numOfStates) {
        super();
        this.containmentMark = new boolean[numOfStates][numOfStates];
        transitions =  new HashMap<>();
        this.numOfStates = numOfStates;
        // initialize transition maps for all
        for(int i = 0; i < numOfStates; i++) {
            transitions.put(i, new HashMap<L, Integer>());
        }
    }

    public void addTransition(int source, L label, int target) {
        if (source == 0){
            setInitialLabel(label);
        }
        HashMap<L, Integer> forwardMap = transitions.get(source);
        getStateNumber(source);
        getStateNumber(target);
        forwardMap.put(label, target);
        if(!labelTransitions.containsKey(label)) {
            labelTransitions.put(label, new HashMap<>());
        }
        HashMap<Integer, Integer> labelMap = labelTransitions.get(label);
        labelMap.put(source, target);
    }

    public boolean isInitialEdge(L label) {
        return initialLabelSet.contains(label);
    }

    public void setInitialLabel(L initialLabel) {
        this.initialLabelSet.add(initialLabel);
    }

    @Override
    public void finalize() {
        // only thing to be performed is to compute the containment relationship
        computeContainmentRelationship();
    }
}
