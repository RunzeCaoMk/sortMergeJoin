# Week 2

The goal of this week is to spec out your project in more detail to help guide you along the way.

## Project Desc

Join is one of the most important functions for a database, and there are many algorithms of join, like nested loop join, hash join and sort-merge join, etc. However, join is a time-consuming operation and usually have large amount of work, in order to improve the performance of join, we have to take advantage of multi-core CPU and do the parallel join. Parallel join have two main algorithms which are parallel sort-merge join and parallel hash join. According to the time limitation, both sort-merge and hash join will not include the optional partition phase.

This project will focus on research and implement two parallel join algorithms, evaluate the performance, compare the difference between them, and find the best algorithm for some given scenarios. The main effort will be figure out the parallel part for two joins and implementing parallel sort-merge join. 


## Project Milestones
 - MS 1: Level 1 in-register sorting and level 2 in-cache sorting. Implement and test sorting network for level 1 and bitonic merge network for level 2.
 - MS 2: Level 3 out-of-cache sorting. Implement and test M-WAY merging.
 - MS 3: Level 3 out-of-cache sorting. Implement and test M-PASS merging.
 - MS 4: Implement and test merge phase for sort-merge algorithm.
 - MS 5: Migrate and refactor the self-defined hash table using Crusty components. Able to finish regular hash-join
   using self-defined hash table with different permutations.
 - MS 6: Evaluation. Design and implement micro-benchmarks for parallel hash join and parallel sort-merge join.
 - MS 7: Paper write up.


## Integration or dependencies
 
 - Dependency for hash algorithms
 - Dependency for self defined hash table.
 - etc.

## Evaluation 
A key goal of this project is to develop a component that you will evaluate for performance and correctness. Write down your plan for evaluating your project in the following sub-sections.

### Correctness
[comment]: <> ([As part of this project you should develop tests for checking correctness, this should include unit tests, integration tests, and possibly end-to-end tests. Write down at a high-level your plan for correctness checking. You do not need to write out a detailed unit testing plan, but higher level functionality you want to test.])
For sort-merge join, the first thing is the algorithm should sort and merge runs correctly, and then merge two relations correctly. Also, different implementations should follow the design. For hash join, the hash hey for each item should be consistent, and the behavior of hash table should correct like extend when reach the load factor.

### Performance
[comment]: <> ([Identify what is the key components or parameters you are going to evaluate &#40;e.g. measuring the impact of special thing X on a write-heavy workload&#41;. Identify key metrics for this evaluation &#40;e.g. ingest throughput, size, query latency, optimization time&#41;. If there is a reasonable baseline, what is that? Your evaluation should either measure the impact of some key parameters or compare against a baseline.])
For the performance, the project will likely test algorithms from throughput and latency two perspectives by using micro-benchmarks. For example, the input size, cardinality, ralative size between two relations, and percentage of same key in two relations.


## Outcomes

[comment]: <> ([With the above information take a few minutes and write down what deliverables of the project/evaluation do you think would make for what grade. This is not a contract that is binding on the outcome/grade, but will be helpful for figuring out what to aim for. If unexpected difficulties arise in the project &#40;which usually does&#41;, we will discuss them and can adjust our expectations on the project.])
 - A on the project: finish all the milestones on time.
 - B on the project: skip or not finish 1 or 2 milestone. 
 - C on the project: skip or not finish more than 3 milestones.