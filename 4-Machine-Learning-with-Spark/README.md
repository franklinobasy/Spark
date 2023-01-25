# Machine Learning with Spark

Just as spark dataframe were designed closely to immitate Python and R dataframes, the SparkML library resembles Scikit-Learn.

SparkML support pipelines, where we can stitch together data cleaning and feature engineering steps with model training and prediction.

![](images/sparkml1.png)

Spark in general, handles algorithms that scale linearly with input data size.

![](images/sparkml2.png)

In the sparkML library, we find support for:

- Supervised learning
- Unspervised learning
- Feature computation
- Hyperparameter tuning
- Model evaluation

## Distributed Machine Learning: Machine Learning in Paralllelization

There are two different ways to achieve parallelization in machine learning:

1. Data parallelization
2. Task parallelization

![](images/sparkml3.png)

In data parallelization, we can use a large dataset and train the same model on smaller subsets of the data in parallel. This is spark's default behaviour. Spark's driver program acts as a perimeter server for most algorithm, where the partial results of each iteration gets combined.

In task parallelization, training of many models is done in parallel on a single dataset small enough to fit on a single machine.

### Feature engineering

Before we can start feeding features into any ML algorithm, we need to make sure that the inputs is in the correct format.
Just as most other ML libraries, SparkML expects numeric input values.
So if you are dealing with categorical variables or free-form text, you will need to transform the features first. Even for numeric data, we might need to rescale a particular feature to enhance certain algorithm performance.

Let's consider an example about how many song a user listened to in the pat hour versus how many days ago they started using the service. If you use a classifier that classifies the distance between the data points based on euclidean distance, the feature divided range may dominate the result.

![](images/sparkml4.png)

This is why we need to normalize our features to the same range, typically between 0 and 1 or -1 and 1.

![](images/sparkml5.png)

Spark supports many different scaling approaches:

**Numerical Variables(Scalers)**:
- [Normalizer](https://spark.apache.org/docs/latest/ml-features.html#normalizer)
- [StandardScaler](https://spark.apache.org/docs/latest/ml-features.html#standardscaler)
- [MinMaxScaler](https://spark.apache.org/docs/latest/ml-features.html#minmaxscaler)
- [MaxAbsScale](https://spark.apache.org/docs/latest/ml-features.html#maxabsscaler)

**Categorical Variables(Indexers)**:
- [StringIndexer](https://spark.apache.org/docs/latest/ml-features.html#stringindexer)
- [IndexToString](https://spark.apache.org/docs/latest/ml-features.html#indextostring)
- [VectorIndexer](https://spark.apache.org/docs/latest/ml-features.html#vectorindexer)


