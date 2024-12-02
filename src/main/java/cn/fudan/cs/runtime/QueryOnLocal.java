package cn.fudan.cs.runtime;

import cn.fudan.cs.input.InputTuple;
import cn.fudan.cs.input.SimpleTextStreamWithExplicitDeletions;
import cn.fudan.cs.input.TextFileStream;
import cn.fudan.cs.stree.data.MonitoredNode;
import cn.fudan.cs.stree.data.TaskCluster;
import cn.fudan.cs.stree.query.Automata;
import cn.fudan.cs.stree.query.ManualQueryAutomata;
import cn.fudan.cs.stree.data.arbitrary.TRDRAPQ;
import cn.fudan.cs.stree.data.arbitrary.TRDNodeRAPQ;
import cn.fudan.cs.stree.engine.RPQEngine;
import cn.fudan.cs.stree.engine.WindowedRPQ;
import cn.fudan.cs.stree.util.Constants;
import cn.fudan.cs.stree.util.Hasher;
import cn.fudan.cs.stree.util.Semantics;
import mpjdev.Status;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import mpi.MPI;

/**
 * @author zhsyy
 * @version 1.0
 * @date 2023/1/7 8:59
 */

public class QueryOnLocal {
    private static Logger logger = LoggerFactory.getLogger(QueryOnLocal.class);
    public static void main(String[] args) {
        MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int rankSize = MPI.COMM_WORLD.Size();

        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(getCLIOptions(), args);
        } catch (ParseException e) {
            logger.error("Command cmd argument can NOT be parsed", e);
            return;
        }

        // parse all necessary command cmd options
        String readFilePath = cmd.getOptionValue("rf", "");
        boolean ifWriteTaskList = cmd.hasOption("wf");
        int hop = Integer.parseInt(cmd.getOptionValue("hop", "1"));
        int threshold = Integer.parseInt(cmd.getOptionValue("thr", "0"));
        int nodeVectorSize = Integer.parseInt(cmd.getOptionValue("nvs", "100"));
        String filePath = cmd.getOptionValue("fp", "./src/main/resources/");
        String fileName = cmd.getOptionValue("f", "sample") + ".txt";
        String queryCase = cmd.getOptionValue("q", "q_0");
        int maxSize = Integer.parseInt(cmd.getOptionValue("ms", "10000"));
        long windowSize = Long.parseLong(cmd.getOptionValue("ws", "7"));
        long slideSize = Long.parseLong(cmd.getOptionValue("ss", "1"));
        int threadCount = Integer.parseInt(cmd.getOptionValue("tc", "10"));
        ManualQueryAutomata<String> query = ManualQueryAutomata.getManualQuery(queryCase);
        TaskCluster.windowSize = windowSize;
        TaskCluster.nodeVectorSize = nodeVectorSize;

        if (rank == 0) {
            // 1. Calculate a cluster center for each timestamp to facilitate modification during sliding window
            // 2. Calculate a cluster center for the entire cluster for comparison
            // 3. I want to know which clusters I have
            // 4. Is the number of clusters infinite?
            // 5. Only clusters with the same root can be put into the same cluster
            long begin = System.currentTimeMillis();
            TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
            stream.open(filePath + fileName, maxSize);

            // Record the number of tuples that have not been processed for each worker, which is used to evaluate the worker's workload
            int[] worker2Workload = new int[rankSize];
            // Record the tuple corresponding to each timestamp to supplement the tuple before sending the initial edge
            HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray = new HashMap<>();
            // Record the number of tuples corresponding to each timestamp
            HashMap<Long, Integer> time2NumberOfTuples = new HashMap<>();
            // worker -> {max time of initial edge} The maximum timestamp of the initial edge on each worker, used to calculate duplicate edges
            HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge = new HashMap<>();

            int numberOfTuples = 0;
            InputTuple<Integer, Integer, String> input;
            input = stream.next();
            LinkedList<InputTuple<Integer, Integer, String>> initialTuplesQueue = new LinkedList<>();
            ConcurrentHashMap<Integer, Integer> freeWorker2TaskCount = new ConcurrentHashMap<>();

            LinkedList<TaskCluster> allTaskList = new LinkedList<>();

            // 读取文件，生成cachedInputArray和initialTuplesQueue
            while (input != null) {
                numberOfTuples++;
                long currentTime = input.getTimestamp();
                if (query.isInitialEdge(input.getLabel())) {
                    initialTuplesQueue.add(input);
                }
                cachedInputArray.putIfAbsent(currentTime, new ArrayList<>());
                cachedInputArray.get(currentTime).add(input);

                time2NumberOfTuples.putIfAbsent(currentTime, 0);
                time2NumberOfTuples.compute(currentTime, (k, v) -> v + 1);

                input = stream.next();
            }
            stream.close();

            // Construct cluster
            long cluster_begin = System.currentTimeMillis();
            LinkedList<Future<TaskCluster>> futures = new LinkedList<>();
            ExecutorService executor = Executors.newFixedThreadPool(30);
            // Use multithreading to concurrently calculate k hop
            for (InputTuple<Integer, Integer, String> initialTuple : initialTuplesQueue) {
                packetCluster packetCluster = new packetCluster(hop, windowSize, query, cachedInputArray, initialTuple);
                futures.add(executor.submit(packetCluster));
            }
            System.out.println("add finish with time " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s");

            Map<Integer, Set<TaskCluster>> root2clusters = new HashMap<>();
            // Add the obtained k hop to the cluster
            while (!futures.isEmpty()) {
                if (futures.peek().isDone()) {
                    try {
                        TaskCluster currentCluster = futures.poll().get();
                        long currentTimestamp = currentCluster.getMaxTimestamp();
                        int root = currentCluster.getRoot();
                        if (root2clusters.containsKey(root)) {
                            TaskCluster targetCluster = null;
                            long targetLength = Long.MIN_VALUE;
                            Set<TaskCluster> clusters = root2clusters.get(root);
                            for (TaskCluster cluster : clusters) {
                                if (cluster.getMaxTimestamp() >= currentTimestamp - windowSize) {
                                    long overlapLength = cluster.calculateOverlapLength(currentCluster);
                                    if (overlapLength > targetLength) {
                                        targetLength = overlapLength;
                                        targetCluster = cluster;
                                    }
                                }
                            }
                            if (targetCluster != null && targetLength >= threshold
                            ) {
                                targetCluster.addNewInitialTask(currentCluster, currentTimestamp);
                            } else {
                                clusters.add(currentCluster);
                                allTaskList.add(currentCluster);
                            }
                        } else {
                            // Create a new cluster
                            root2clusters.put(root, new HashSet<>());
                            root2clusters.get(root).add(currentCluster);
                            allTaskList.add(currentCluster);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            initialTuplesQueue.clear();
            executor.shutdown();
            System.out.println("clustering time cost = " + (System.currentTimeMillis() - cluster_begin) * 1.0 / 1000 + "s with size=" + allTaskList.size());

            Random random = new Random();
            Collections.shuffle(allTaskList, random);

            // Send all tuples in advance
            ArrayList<InputTuple<Integer, Integer, String>> allTuple = new ArrayList<>();
            Set<Long> allTimestamp = new HashSet<>();
            for (ArrayList<InputTuple<Integer, Integer, String>> tuples : cachedInputArray.values()) {
                allTuple.addAll(tuples);
                for (InputTuple<Integer, Integer, String> tuple : tuples) {
                    allTimestamp.add(tuple.getTimestamp());
                }
            }
            for (int i = 1; i < rankSize; i++) {
                MPI.COMM_WORLD.Isend(allTuple.toArray(), 0, allTuple.size(), MPI.OBJECT, i, Constants.TUPLES);
                worker2AllTimeOfInitialEdge.putIfAbsent(i, allTimestamp);
            }
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Hot start
            for (int targetWorker = 1; targetWorker < rankSize; targetWorker++) {
                int sendCount = 0;
                List<InputTuple<Integer, Integer, String>> initialEdges = new ArrayList<>();
                while (!allTaskList.isEmpty()) {
                    TaskCluster taskCluster = allTaskList.poll();
                    sendCount += taskCluster.getInitialEdgeList().size();
                    initialEdges.addAll(taskCluster.getInitialEdgeList());
                    if (sendCount > 1000) {
                        break;
                    }
                }
                sendQueryTasks(windowSize, cachedInputArray, worker2AllTimeOfInitialEdge, targetWorker, initialEdges, worker2Workload);
            }
            System.out.println("Hot Start Send Tuples = " + Arrays.toString(worker2Workload));

            AtomicBoolean ifCollectFreeWorkers = new AtomicBoolean(true);
            Thread collectFreeWorkers = new Thread(() -> {
                // After the hot start is finished, send a signal to tell the worker to record data and start monitoring unFinishedTaskList
                for (int i = 1; i < rankSize; i++) {
                    MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.MONITOR_WORKLOAD);
                }
                while (ifCollectFreeWorkers.get()) {
                    Status status = MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, Constants.FREE_WORKER);
                    if (status != null) {
                        int[] taskSize = new int[1];
                        MPI.COMM_WORLD.Recv(taskSize, 0, 1, MPI.INT, status.source, Constants.FREE_WORKER);
                        freeWorker2TaskCount.put(status.source, taskSize[0]);
                    }
                }
            });
            collectFreeWorkers.start();

            int callCount = 0;
            while (true) {
                if (!freeWorker2TaskCount.isEmpty()) {
                    if (!allTaskList.isEmpty()) {
                        int targetWorker = -1;
                        targetWorker = freeWorker2TaskCount.keySet().iterator().next();
                        if (targetWorker != -1) {
                            List<InputTuple<Integer, Integer, String>> candidateSendInitialEdges = new LinkedList<>();
                            int count = 0;
                            while (!allTaskList.isEmpty()) {
                                List<InputTuple<Integer, Integer, String>> currentTask = allTaskList.poll().getInitialEdgeList();
                                candidateSendInitialEdges.addAll(currentTask);
                                count += currentTask.size();
                                if (count > 1000)
                                    break;
                            }
                            random = new Random();
                            Collections.shuffle(candidateSendInitialEdges, random);
                            freeWorker2TaskCount.remove(targetWorker);
                            sendQueryTasks(windowSize, cachedInputArray, worker2AllTimeOfInitialEdge, targetWorker, candidateSendInitialEdges, worker2Workload);
                        }
                    } else {
                        //Send NO_MORE_TASK signal
                        System.out.println("send NO_MORE_TASK signal to workers ");
                        for (int i = 1; i < rankSize; i++) {
                            MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.NO_MORE_TASK);
                        }
                        break;
                    }
                }
            }
//            writeObjectToFile(tupleList, "./savedTuplesInEachWorker/tupleList.obj");
            System.out.println("Final Send Tuples = " + Arrays.toString(worker2Workload) + " call count = " + callCount);
            ifCollectFreeWorkers.set(false);
            for (int i = 1; i < rankSize; i++) {
                MPI.COMM_WORLD.Isend(new int[]{}, 0, 0, MPI.INT, i, Constants.WORKER_FINISH);
            }

            double totalTime = (System.currentTimeMillis() - begin) * 1.0 /1000;
            logger.info("average speed of sending tuples = " + numberOfTuples / totalTime + "tuples/s, time cost = " + totalTime + "s");
        }
        else {
            long begin = System.currentTimeMillis();
            AtomicLong processBegin = new AtomicLong();
            AtomicInteger unProcessedTupleCount = new AtomicInteger(0);
            RPQEngine<String> rapqEngine = new WindowedRPQ<String, TRDRAPQ<Integer>, TRDNodeRAPQ<Integer>>
                    (query, 10000, windowSize, slideSize, threadCount, Semantics.ARBITRARY, rank, unProcessedTupleCount);

            ConcurrentLinkedQueue<InputTuple<Integer, Integer, String>> receivedTuples = new ConcurrentLinkedQueue<>();
            Thread receiveMsgTag = new Thread(() -> {
                try {
                    boolean isFinish = false;
                    while (!isFinish) {
                        Status status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, MPI.ANY_TAG);
                        int tag = status.tag;

                        switch (tag) {
                            case Constants.INITIAL_EDGE:
                                int[] size = new int[1];
                                MPI.COMM_WORLD.Recv(size, 0, status.count, MPI.INT, 0, Constants.INITIAL_EDGE);
                                unProcessedTupleCount.addAndGet(size[0]);
                                WindowedRPQ.receiveNewTask = true;
//                                System.out.println("worker " + rank + " receive " + size[0] + " new initial edge and set receiveNewTask to true");
                                break;
                            case Constants.TUPLES:
                                InputTuple<Integer, Integer, String>[] tuples = new InputTuple[status.count];
                                MPI.COMM_WORLD.Recv(tuples, 0, status.count, MPI.OBJECT, 0, Constants.TUPLES);

                                Collections.addAll(receivedTuples, tuples);
                                break;
                            case Constants.MONITOR_WORKLOAD:
                                processBegin.set(System.currentTimeMillis());
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.MONITOR_WORKLOAD);
                                ((WindowedRPQ) rapqEngine).beginMonitorTaskList();
//                            MPI.COMM_WORLD.Gather(new int[]{rapqEngine.getUnFinishedTaskListSize()}, 0, 1, MPI.INT, new int[rankSize], 0, 1, MPI.INT, 0);
                                break;
                            case Constants.WORKER_FINISH:
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.WORKER_FINISH);
//                                System.out.println(rank + " - receive FINISH tag");
                                isFinish = true;
                                break;
                            case Constants.NO_MORE_TASK:
//                                System.out.println("worker " + rank + " receive NO_MORE_TASK tag");
                                MPI.COMM_WORLD.Irecv(new int[]{}, 0, status.count, MPI.INT, 0, Constants.NO_MORE_TASK);
                                break;
                            default:
                                break;
                        }
                    }
                } catch (Exception e) {
                    System.out.println("worker " + rank + ": " + e);
                    e.printStackTrace();
                }
            });
            receiveMsgTag.start();

            int numberOfReceivedTuples = 0;
            while (receiveMsgTag.isAlive()) {
                int count = 0;
                while (!receivedTuples.isEmpty()) {
                    InputTuple<Integer, Integer, String> inputTuple = receivedTuples.poll();
                    numberOfReceivedTuples++;
                    rapqEngine.processEdge(inputTuple);

                    if (inputTuple.isInitialEdge()) {
                        count++;
                    }
                }
                unProcessedTupleCount.addAndGet(-count);
            }
            rapqEngine.shutDown();
            double processTime = (System.currentTimeMillis() - processBegin.get()) * 1.0 /1000;
            logger.info("worker " + rank + ": average speed of processing tuples = " + numberOfReceivedTuples / processTime + "tuples/s, process time cost = " + processTime + "s." );
//            System.out.println(rapqEngine.getResults().getResultSize());
        }
            MPI.Finalize();
            System.out.println("rank " + rank +" end!");
            System.exit(0);
    }

    private static void sendQueryTask(long windowSize, HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray, HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge, int targetWorker, InputTuple<Integer, Integer, String> inputTuple, int[] worker2Workload) {
        ArrayList<InputTuple<Integer, Integer, String>> candidateSendTuples = new ArrayList<>();
        // new一个新的的目的是为了不修改cachedInputArray中的tuple
        inputTuple = new InputTuple<>(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
        inputTuple.setInitialEdge(true);
        candidateSendTuples.add(inputTuple);
        // max(t_1) <= currentTime <= min(t_2)
        long currentTime = inputTuple.getTimestamp();
        // 最大的小于currentTime的ts
        long t_1 = Long.MIN_VALUE;
        // 最小的大于currentTime的ts
        long t_2 = Long.MAX_VALUE;
        Set<Long> tsList = worker2AllTimeOfInitialEdge.get(targetWorker);
        if (tsList != null) {
            for (Long ts : tsList) {
                if (ts <= currentTime) {
                    if (ts > t_1)
                        t_1 = ts;
                }
                if (ts >= currentTime) {
                    if (ts < t_2)
                        t_2 = ts;
                }
            }
        }

        for (long k = Long.max(currentTime - windowSize, t_1 + windowSize + 1); k <= Long.min(currentTime + windowSize, t_2 - windowSize - 1); k++) {
            if (cachedInputArray.containsKey(k)) {
                candidateSendTuples.addAll(cachedInputArray.get(k));
            }
        }
        worker2Workload[targetWorker] += candidateSendTuples.size();
        MPI.COMM_WORLD.Isend(new int[]{1}, 0, 1, MPI.INT, targetWorker, Constants.INITIAL_EDGE);
        MPI.COMM_WORLD.Isend(candidateSendTuples.toArray(), 0, candidateSendTuples.size(), MPI.OBJECT, targetWorker, Constants.TUPLES);
        worker2AllTimeOfInitialEdge.putIfAbsent(targetWorker, new HashSet<>());
        worker2AllTimeOfInitialEdge.get(targetWorker).add(currentTime);
        System.out.println("coordinator sends " + 1 + " initial edges with " + candidateSendTuples.size() +  " tuples to worker " + targetWorker);
        if (1 != candidateSendTuples.size())
            System.out.println("not equal!!!");
    }

    private static void sendQueryTasks(long windowSize,
                                       HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray,
                                       HashMap<Integer, Set<Long>> worker2AllTimeOfInitialEdge,
                                       int targetWorker,
                                       List<InputTuple<Integer, Integer, String>> inputTuples,
                                       int[] worker2Workload) {
        ArrayList<InputTuple<Integer, Integer, String>> candidateSendTuples = new ArrayList<>();
        for (InputTuple<Integer, Integer, String> inputTuple : inputTuples) {
            // The purpose of new is to not modify the tuple in cachedInputArray
            inputTuple = new InputTuple<>(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
            inputTuple.setInitialEdge(true);
            candidateSendTuples.add(inputTuple);

            // max(t_1) <= currentTime <= min(t_2)
            long currentTime = inputTuple.getTimestamp();
            long t_1 = Long.MIN_VALUE;
            long t_2 = Long.MAX_VALUE;
            Set<Long> tsList = worker2AllTimeOfInitialEdge.get(targetWorker);
            if (tsList != null) {
                for (Long ts : tsList) {
                    if (ts <= currentTime) {
                        if (ts > t_1)
                            t_1 = ts;
                    }
                    if (ts >= currentTime) {
                        if (ts < t_2)
                            t_2 = ts;
                    }
                }
            }

            for (long k = Long.max(currentTime - windowSize, t_1 + windowSize + 1); k <= Long.min(currentTime + windowSize, t_2 - windowSize - 1); k++) {
                if (cachedInputArray.containsKey(k)) {
                    candidateSendTuples.addAll(cachedInputArray.get(k));
                }
            }
            worker2AllTimeOfInitialEdge.putIfAbsent(targetWorker, new HashSet<>());
            worker2AllTimeOfInitialEdge.get(targetWorker).add(inputTuple.getTimestamp());
        }

        worker2Workload[targetWorker] += candidateSendTuples.size();
        MPI.COMM_WORLD.Isend(new int[]{inputTuples.size()}, 0, 1, MPI.INT, targetWorker, Constants.INITIAL_EDGE);
        MPI.COMM_WORLD.Isend(candidateSendTuples.toArray(), 0, candidateSendTuples.size(), MPI.OBJECT, targetWorker, Constants.TUPLES);
//        System.out.println("coordinator sends " + inputTuples.size() + " initial edges with " + candidateSendTuples.size() +  " tuples to worker " + targetWorker);
        if (inputTuples.size() != candidateSendTuples.size())
            System.out.println("not equal!!!");
    }

    private static Options getCLIOptions() {
        Options options = new Options();
        options.addOption("alpha", "alpha", true, "alpha");
        options.addOption("hop", "hop", true, "hop");
        options.addOption("thr", "threshold", true, "threshold");
        options.addOption("f", "file", true, "Text file to read");
        options.addOption("fp", "file-path", true, "Directory to store datasets");
        options.addOption("q", "query-case", true, "Query case");
        options.addOption("ms", "max-size", true, "Maximum size to be processed");
        options.addOption("ws", "window-size", true, "Size of the window");
        options.addOption("ss", "slide-size", true, "Slide of the window");
        options.addOption("tc", "threadCount", true, "# of Threads for inter-query parallelism");
        options.addOption("gt", "garbage-collection-threshold", true, "Threshold to execute DGC");
        options.addOption("ft", "fixed-throughput", true, "Fixed fetch rate from dataset");
        options.addOption("tt", "file", false, "Test throughput");
        options.addOption("wf", "file", false, "Write File");
        options.addOption("rf", "file", true, "Read File");
        options.addOption("nvs", "nvs", true, "nvs");
        return options;
    }

    static class packetCluster implements Callable<TaskCluster> {
        int hop;
        long windowSize;
        Automata<String> automata;
        HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray;
        InputTuple<Integer, Integer, String> initialEdge;

        public packetCluster(int hop, long windowSize, Automata<String> automata, HashMap<Long, ArrayList<InputTuple<Integer, Integer, String>>> cachedInputArray, InputTuple<Integer, Integer, String> initialEdge) {
            this.hop = hop;
            this.windowSize = windowSize;
            this.automata = automata;
            this.cachedInputArray = cachedInputArray;
            this.initialEdge = initialEdge;
        }

        @Override
        public TaskCluster call() {
            // This is the collection of tuples that need to be traversed
            List<InputTuple<Integer, Integer, String>> allCachedTuples = new LinkedList<>();
            for (long i = Long.max(initialEdge.getTimestamp() - windowSize, 0); i <= initialEdge.getTimestamp() + windowSize; i++) {
                if (cachedInputArray.containsKey(i))
                    allCachedTuples.addAll(cachedInputArray.get(i));
            }

            int initialNode = initialEdge.getTarget();
            Set<Hasher.MapKey<Integer>> cachedNode = new HashSet<>();
            HashMap<Hasher.MapKey<Integer>, MonitoredNode> allNode = new HashMap<>();
            cachedNode.add(Hasher.createTreeNodePairKey(initialNode, 1));
            allNode.put(Hasher.createTreeNodePairKey(initialNode, 1),
                    new MonitoredNode(initialNode, 1, initialEdge.getTimestamp()+windowSize, initialEdge.getTimestamp()));
            // Traverse hop times
            while (hop != 1){
                Set<Hasher.MapKey<Integer>> newCachedNode = new HashSet<>();
                for (InputTuple<Integer, Integer, String> tuple : allCachedTuples) {
                    long edgeTime = tuple.getTimestamp();
                    Map<Integer, Integer> transitions = automata.getTransition(tuple.getLabel());
                    for (Map.Entry<Integer, Integer> states : transitions.entrySet()) {
                        int sourceState = states.getKey();
                        int targetState = states.getValue();
                        if (cachedNode.contains(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState))){
                            MonitoredNode parentNode = allNode.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getSource(), sourceState));
                            newCachedNode.add(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState));
                            if (allNode.containsKey(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState))) {
                                allNode.get(Hasher.getThreadLocalTreeNodePairKey(tuple.getTarget(), targetState)).addNewTimes(parentNode, edgeTime, windowSize);
                            } else {
                                // Calculate whether ts intersects
                                MonitoredNode newNode = new MonitoredNode(tuple.getTarget(), targetState, parentNode, edgeTime, windowSize);
                                // Only assign a value when it is not empty
                                if (!newNode.getMaxTimes().isEmpty())
                                    allNode.put(Hasher.createTreeNodePairKey(tuple.getTarget(), targetState), newNode);
                            }
                        }
                    }
                }
                cachedNode = newCachedNode;
                hop--;
            }
            return calculateCluster(initialEdge.getSource(), initialEdge.getTimestamp(), initialEdge, allNode.values());
        }

        private TaskCluster calculateCluster(int root, long currentTimestamp, InputTuple<Integer, Integer, String> initialTuple, Collection<MonitoredNode> nodes) {
            Map<Long, Integer>[] bucket2TimestampAndCount = new HashMap[TaskCluster.nodeVectorSize];
            for (MonitoredNode node : nodes) {
                int index = node.hashCode() % TaskCluster.nodeVectorSize;
                Iterator<Long> iteratorMax = node.getMaxTimes().iterator();
                Iterator<Long> iteratorMin = node.getMinTimes().iterator();
                while (iteratorMax.hasNext()) {
                    long max = iteratorMax.next();
                    long min = iteratorMin.next();
                    for (long i = min; i <= max; i++) {
                        if (bucket2TimestampAndCount[index] == null)
                            bucket2TimestampAndCount[index] = new HashMap<>();
                        bucket2TimestampAndCount[index].putIfAbsent(i, 0);
                        bucket2TimestampAndCount[index].computeIfPresent(i, (k, v) -> v + 1);
                    }
                }
            }
            return new TaskCluster(root, currentTimestamp, initialTuple, bucket2TimestampAndCount);
        }
    }

    public static void writeObjectToFile(Object obj, String filePath) {
        try (FileOutputStream fileOut = new FileOutputStream(filePath);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            out.writeObject(obj);
            System.out.println("Object has been serialized to " + filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object readObjectFromFile(String filePath) {
        try (FileInputStream fileIn = new FileInputStream(filePath);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}