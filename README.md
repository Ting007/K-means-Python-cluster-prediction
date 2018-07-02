# K means python program for cluster prediction of patient

The “p4.csv” dataset contains 6 patient characteristics: pid, height, weight, waist, diasbp (Diastolic Blood Pressure), and systbp (Systolic Blood Pressure). <br />

Use Spark K-means to cluster the patients into 15 groups (in 100 iterations) according to these patient characteristics except pid.<br />

Compute Within Set Sum of Squared Errors.<br />

Output the centers of the resulted clusters. <br />

Predict the cluster each patient belongs to. <br />

make sure you have p4.csv at hdfs<br />	
To run the python file Q4,py:	<br />
python ./Q4.py the/path/to/p4.csv > path/for/output 	<br />
For example, if p4 is at the home directory of hdfs hadoop, you can run it as:	<br />
python ./Q4.py p4.csv > output.txt <br />
