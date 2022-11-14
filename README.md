# **LOCATION AWARE SCIENTIFIC WORKFLOWS**

A Nextflow library that can be used on any existing workflow. This library reduces the execution time of a workflow executed on a distributed cluster environment. With our limited testing it was found that the library reduces the execution time by at least 50% under all conditions.

| Developers: |
| ----------- |
| Tristan Lilford | 
| Robin Jonker | 

## **Description:**
This repository entails the design and implementation of a location aware scientific workflow in Nextflow. The full development process along with weekly Sprints' documentation is found within this repository.

A breakdown of the filing structure of this repository:

- coding_templates
The final dynamic version of the library in an easy to use template can be found in the coding_templates folder along with the initial iteration of the static version.
- documentation
The project's meeting minutes, project conventions, project plan and scrum boards can be found in the documentation folder.
- official_testing
The official testing of two different workflows can be found in the official_testing folder.
- basic_model_testing
The different versions of our code as we incrementally made changes to it along with testing with other workflows can be found in the basic_model_testing folder.
- data
The data required for one of the workflows within the basic_model_testing folder can be found in the data folder.

## **Development:**

The library is coded in Nextflow. Groovy and Bash is used for functions and processing scripts within the library.

## **Summary:**

In order to use the current version of the library, the sample of the library can be found in the file labeled as *LocAwareSample.nf* within the coding_templates folder. Within the file there is comments guiding you on how to integrate the library with your existing workflow.

## **Testing Results:**

Two sets of official tests were conducted on the versions described above. The first test was conducted when the cluster was relatively idle. It was determined to be relatively idle if less than 10 of the possible nodes were allocated. During this testing each version was run 10 times. The second set of tests were done when the cluster was considered to be busy. It was determined to be busy if more than 10 of the possible nodes were allocated.  A busy testing environment was created by queuing up other data intensive jobs to the cluster system. During this testing each version was run 5 times. 

The average execution time for the different versions under the different conditions are as follows:

Under unloaded conditions:

Non-library: 814.5 seconds
Static version: 275.3 seconds
Dynamic version: 271 seconds

Under loaded conditions:

Non-library: 539.4 seconds
Static version: 1026.6 seconds
Dynamic version: 268.6 seconds

This shows that the dynamic version is the best version.

In each run the number of idle, mixed and allocated nodes were noted as well as the execution time. During all tests the number of idle, mixed and allocated nodes were fairly consistent. The average number of nodes idle, mixed and allocated can be seen in Table 1 below. The highest standard deviation of a node in a certain state was 1,07. This demonstrates the consistency within the testing process. The rest of these results can be found in Table 2.

\begin{table}[H]
    \caption{Average Node States During Testing\label{tab:fonts}}
    \begin{center}
        \begin{tabular}{p{18mm}c1{4.5mm}c2{4.5mm}c3{4.5mm}c4{4.5mm}}
        \hline
               {\msbf Library}       &     {\msbf Testing Set}   &   {\msbf Idle}&     {\msbf Mixed}   &   {\msbf Allocated} \
        \hline
        \msbf Dynamic             & Idle test & 18,2 & 11,9 & 4,9 \
        \msbf Static          & Idle test & 18,5 & 11,9 & 4,6 \
        \msbf Simple       & Idle test & 19,5 & 11,3 & 4,4 \

        \msbf Dynamic             & Busy test & 10,2 & 10,4 & 15,4 \
        \msbf Static          & Busy test & 10,6 & 11,0 & 14,4 \
        \msbf Simple       & Busy test & 9,4 & 9,6 & 17 \


        \hline
        \end{tabular}
    \end{center}
\end{table}

\begin{table}[H]
    \caption{Standard deviation of node states during testing\label{tab:fonts}}
    \begin{center}
        \begin{tabular}{p{23mm}c1{4.5mm}c2{4.5mm}c3{4.5mm}c4{4.5mm}}
        \hline
               {\msbf Testing set}       &     {\msbf SD of idle nodes}   &   {\msbf SD of mixed nodes}&     {\msbf SD of allocated nodes}\
        \hline
        \msbf Idle tests             & 0,56 & 0,28 & 0,21  \
        \msbf Busy Tests          & 0,50 & 0,57 & 1,07 \
        \hline
        \end{tabular}
    \end{center}
\end{table}