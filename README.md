# Big-data-analytics-in-healthcare
Recent development of data mining and machine learning in healthcare has facilitated to build models to solve clinical problems such as prediction of mortality, phenotype classification, etc. The available comprehensive large databases always contain millions of patient records, and it will take a long time to preprocess the data using traditional approach. 
In this project, we apply big data analytic tool, Spark, to conduct quick data preparation. On this basis, we build machine learning models and recurrent neural network models on time series data to predict in-hospital mortality of ICU stays. 
We evaluate the effect of different feature sets and the data coming from different time episodes on the model performance. Our study shows that first 48Hrs data is more meaningful towards predicting in-hospital mortality of ICU stays than first 24Hrs data. In the traditional machine learning models, the models that are trained with additional clinical features perform better. The GRU-based RNN model show better performance than the baselines.
### Data
MIMIC-III is a publicly available critical care database which integrates deidentified, comprehensive clinical data of patients admitted to an Intensive Care Unit (ICU) at the Beth Israel Deaconess Medical Center (BDMC) in Boston, Massachusetts during 2001 to 2012.  
We compiled a subset of the MIMIC-III database containing clinical events that correspond to 17 clinical variables that are commonly used. In addition, we add 5 more patient features: demographic variables including sex and age, and other 3 clinical variables including white blood cells count, potassium level, serum urea nitrogen level, which are chosen from the features that are used in the calculation of SAPS-II score.  
We compared the first 24 hours data and first 48 hours data to determine which data is more meaningful toward predicting in-hospital mortality.
### Model
Machine learning models: logistic regression, SVM  
RNN model: GRU-based RNN model
#### Main reference: 
H. Harutyunyan, H. Khachatrian, D.C. Kale, A. Galstyan, Multitask Learning and Benchmarking with Clinical Time Series Data, 2019, Scientific Data 2019, 6:96
