# D-MWP: Distributed Evaluation of Regular Path Queries On Streaming Graphs

Persistent Regular Path Query (RPQ) on streaming graphs is useful in many online analysis applications. Existing solutions are designed for the single-worker scenario, while scaling-out to distributed processing RPQs on multiple workers will be desirable when the data is large and comes fast. The challenge is how to execute the query with multiple workers to reduce communication overhead and provide sufficient speedup when the number of workers increases. In this paper, a novel strategy called D-MWP is introduced to tackle the issue based on the TRD framework in the literature. TRD is basically a rooted digraph with timestamps that maintains all the initial partial matches of the regular expression.
D-MWP carefully divides the incoming graph edges into distinct query tasks determined by the edges that start potential matches of the regular expression and dynamically balances the workload of the workers. To reduce the redundant maintenance processes in different workers, a grouping method is introduced that evaluates the possibility of sharing maintenance processes among different query tasks and sends the query tasks that are likely to share maintenance processes to the same workers. Extensive experiments on real-world graph datasets demonstrate that D-MWP is efficient and scalable, and the above grouping method is particularly effective, with almost doubled throughput in many cases.

# License

This archive is free for use for academic and non-profit purposes, but if you use it, please reference it properly.

# Disclaimer

The code is provided without warranty of any kind. While we thoroughly tested all code bases on Ubuntu 20.04 LTS (Windows Subsystem of Linux), we do not guarantee that they are exempt from bugs, nor that they will work on other platforms. If you encounter any issues with the code, please feel free to propose them on the ISSUE page of this repo. We will do our best to address your concerns but do not promise to resolve all issues.

# Installation

## Prerequisite

All requirements for libraries are included in the pom.xml file and the lib folder in path **/src/resources/**. After importing the project, please first follow all required dependencies.

## Build & Run

This is a Java project. The Java version we used for testing is 8, however, we subjectively believe that it can run on relatively lower versions.

*If you have access to an IDE like IntelliJ IDEA*, just open the project and work with it and build it automatically.  Note that we conducted experiments on the server by packaging the code into a jar package. The dependencies and information required to package the jar package are included in the pom.xml file and the lib folder.

*Other*, please see instructions below.

The mpi version we use is mpj v_0.43, which can be downloaded from [http://mpjexpress.org/download.php](https://). Then configure mpj and add it to the project library to compile and run. The lib folder in the resource file directory of the project also provides the lib files we use. If you use IDEA to open the file, you can directly add it to the project library. Note that when running this project in a distributed environment, you need to package it into a jar file and run it using the relevant commands in mpj.

The usage of mpj is detailed in the official website document [http://mpjexpress.org/guides.html](https://). For the convenience of users, we summarize the contents of the above documents as follows:

1. Configure password-free login for all workers in the cluster and configure the Java environment.
2. Download the relevant folders of mpj v0.43 on each worker and configure the environment variable export `MPJ_HOME=/home/usr/mpj-v0_43`.
3. Place the project jar file in the same directory of all workers.
4. Create a text file machine in the same directory as the project jar file in the coordinator, and write all the IP addresses of the coordinator and the worker into it. An example is:

   ```
   192.168.0.1
   192.168.0.2
   xxx
   192.168.0.8
   ```

   where 192.168.0.1 is the IP address of the coordinator, and the others are the IP addresses of the workers.
5. Use the command mpjboot machine to start mpj on the cluster.
6. Use the command `mpjrun.sh -dev niodev -np $numberOfWorkers -jar projectName.jar -parameters xxxx` to run the project in a distributed environment.

If you do not need to run the project in a distributed system environment, just use `mpjrun.sh -np \$numberOfWorkers -jar projectName.jar -parameters xxxx`, where `$numberOfWorkers` is the number of processes used. In addition, we strongly recommend adding JVM memory control to the command. On a server with 256G memory, we set the JVM to `-Xmx230g -Xms20g` to maximize the performance.

We provide a runnable script (run.sh) to run our project after configuring the environment.

## Hints

1. All configurations are written in `pom.xml`. The information of the dataset has been given in the paper. To complete your task, please read through and install all dependency files.
2. The project structure mainly contains 5 modules. The input package mainly handles file reading operations; the runtime package is the entry point for code running, which temporarily only contains a QueryOnLocal.java file; under the stree package, the data package contains the implementation of TRD ; The engine package handles the expansion and maintenance of TRD; the query package contains the implementation of DFA and NFA (which we will expand in the future); the util package contains various tool classes, including constants for environment parameter configuration, and classes for monitoring data.
3. For the parameters passed in when the code is running, please refer to the instructions in the `runtime/QueryOnLocal.java` file. We also provide a brief introduction to the parameters at the end of the article.

All parameters are set to default values. You do not need to specify them when running on a single machine. Note that all parameters are set to default values. You do not need to specify them when running on a single machine. When running in a distributed environment, you need to specify `-fp` and `-f`.
4. For configuration during project running, please view `stree/util/Constants.java`,

## Datasets

The data set Stack Overflow we used in the paper is free and open source for download.

Please see [https://stackexchange.com/](https://) to learn how to download and use it. We also provide a sample dataset, which is located in the `/src/main/resource/` directory

## Issue

If you have any problem, feel free to write your questions in ISSUE.

## Parameter settings


| **Option** | **Long Name**                  | **Argument** | **Description**                          |
| ---------- | ------------------------------ | ------------ | ---------------------------------------- |
| `alpha`    | `alpha`                        | Yes          | Alpha                                    |
| `hop`      | `hop`                          | Yes          | Hop                                      |
| `thr`      | `threshold`                    | Yes          | Threshold                                |
| `f`        | `file`                         | Yes          | Text file to read                        |
| `fp`       | `file-path`                    | Yes          | Directory to store datasets              |
| `q`        | `query-case`                   | Yes          | Query case                               |
| `ms`       | `max-size`                     | Yes          | Maximum size to be processed             |
| `ws`       | `window-size`                  | Yes          | Size of the window                       |
| `ss`       | `slide-size`                   | Yes          | Slide of the window                      |
| `tc`       | `threadCount`                  | Yes          | # of Threads for inter-query parallelism |
| `gt`       | `garbage-collection-threshold` | Yes          | Threshold to execute DGC                 |
| `ft`       | `fixed-throughput`             | Yes          | Fixed fetch rate from dataset            |
| `tt`       | `file`                         | No           | Test throughput                          |
| `wf`       | `file`                         | No           | Write File                               |
| `rf`       | `file`                         | Yes          | Read File                                |
| `nvs`      | `nvs`                          | Yes          | NVS                                      |
