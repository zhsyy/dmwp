package cn.fudan.cs.stree.data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author zhsyy
 * @version 1.0
 * @date 2024/10/5 15:34
 */
public class MonitoredNode {
    int vertex;
    int state;
    int size;
    List<Long> maxTimes = new LinkedList<>();
    List<Long> minTimes = new LinkedList<>();

    public MonitoredNode(int vertex, int state, long maxTime, long minTime) {
        this.vertex = vertex;
        this.state = state;
        this.maxTimes.add(maxTime);
        this.minTimes.add(minTime);
        size++;
    }

    public MonitoredNode(int vertex, int state, MonitoredNode parentNode, long edgeTime, long windowSize) {
        this.vertex = vertex;
        this.state = state;
        addNewTimes(parentNode, edgeTime, windowSize);
    }

    public void addNewTimes(MonitoredNode parentNode, long edgeTime, long windowSize) {
        if (!this.equals(parentNode)) {
            Iterator<Long> maxTimeIterator = parentNode.maxTimes.iterator();
            Iterator<Long> minTimeIterator = parentNode.minTimes.iterator();
            while (maxTimeIterator.hasNext()) {
                long max = maxTimeIterator.next();
                long min = minTimeIterator.next();
                if (edgeTime <= max && edgeTime + windowSize >= min) {
                    this.maxTimes.add(Long.min(max, edgeTime + windowSize));
                    this.minTimes.add(Long.max(edgeTime, min));
                }
            }
        }
    }

    public List<Long> getMaxTimes(){
        return maxTimes;
    }
    public List<Long> getMinTimes(){
        return minTimes;
    }

    @Override
    public String toString() {
        return "(" + vertex +", " + state + ")" + "<max:"+ maxTimes + ", min:"+minTimes+">";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MonitoredNode other = (MonitoredNode) obj;
        return vertex == other.vertex && state == other.state;
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + vertex;
        h = 31 * h + state;
        return h;
    }
}
