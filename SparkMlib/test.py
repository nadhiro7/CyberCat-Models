# Here are some imports that are used along this notebook
import os
import math
import itertools
import multiprocessing
import pandas
import numpy as np
import pandas as pd
from time import time
from collections import OrderedDict
gt0 = time()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row


conf = SparkConf()\
    .setMaster(f"local[{multiprocessing.cpu_count()}]")\
    .setAppName("PySpark NSL-KDD")\
    .setAll([("spark.driver.memory", "8g"), ("spark.default.parallelism", f"{multiprocessing.cpu_count()}")])

# Creating local SparkContext with specified SparkConf and creating SQLContext based on it
sc = SparkContext.getOrCreate(conf=conf)
sc.setLogLevel('INFO')
sqlContext = SQLContext(sc)


from pyspark.sql.types import *
from pyspark.sql.functions import udf, split, col
import pyspark.sql.functions as sql
# file = open('sparkml_results.txt', 'w')
train20_dataset_path = "hdfs://127.0.0.1:9000/mlcic/train_20.csv"
train_dataset_path = "hdfs://127.0.0.1:9000/mlcic/train2.csv"
test_dataset_path = "hdfs://127.0.0.1:9000/mlcic/test2.csv"

col_names = np.array([
    'flow_duration', 'Header_Length', 'Protocol Type', 'Duration',
       'Rate', 'Srate', 'Drate', 'fin_flag_number', 'syn_flag_number',
       'rst_flag_number', 'psh_flag_number', 'ack_flag_number',
       'ece_flag_number', 'cwr_flag_number', 'ack_count',
       'syn_count', 'fin_count', 'urg_count', 'rst_count',
    'HTTP', 'HTTPS', 'DNS', 'Telnet', 'SMTP', 'SSH', 'IRC', 'TCP',
       'UDP', 'DHCP', 'ARP', 'ICMP', 'IPv', 'LLC', 'Tot sum', 'Min',
       'Max', 'AVG', 'Std', 'Tot size', 'IAT', 'Number', 'Magnitue',
       'Radius', 'Covariance', 'Variance','Weight','label'
])

nominal_inx = [46]
binary_inx = [7,8,9,10, 11, 12,13,  19,20, 21,23,24,25,26,27,28,29,30,31,32]
numeric_inx = list(set(range(46)).difference(binary_inx))

nominal_cols = col_names[nominal_inx].tolist()
binary_cols = col_names[binary_inx].tolist()
numeric_cols = col_names[numeric_inx].tolist()


def load_dataset(path):
    dataset_rdd = sc.textFile(path, 8).map(lambda line: line.split(','))
    dataset_df = (dataset_rdd.toDF(col_names.tolist()).select(
                    col('flow_duration').cast(DoubleType()),
                    col('Header_Length').cast(DoubleType()),
                    col('Protocol Type').cast(DoubleType()),
                    col('Duration').cast(DoubleType()),
                    col('Rate').cast(DoubleType()),
                    col('Srate').cast(DoubleType()),
                    col('Drate').cast(DoubleType()),
                    col('fin_flag_number').cast(DoubleType()),
                    col('syn_flag_number').cast(DoubleType()),
                    col('rst_flag_number').cast(DoubleType()),
                    col('psh_flag_number').cast(DoubleType()),
                    col('ack_flag_number').cast(DoubleType()),
                    col('ece_flag_number').cast(DoubleType()),
                    col('cwr_flag_number').cast(DoubleType()),
                    col('ack_count').cast(DoubleType()),
                    col('syn_count').cast(DoubleType()),
                    col('fin_count').cast(DoubleType()),
                    col('urg_count').cast(DoubleType()),
                    col('rst_count').cast(DoubleType()),
                    col('HTTP').cast(DoubleType()),
                    col('HTTPS').cast(DoubleType()),
                    col('DNS').cast(DoubleType()),
                    col('Telnet').cast(DoubleType()),
                    col('SMTP').cast(DoubleType()),
                    col('SSH').cast(DoubleType()),
                    col('IRC').cast(DoubleType()),
                    col('TCP').cast(DoubleType()),
                    col('UDP').cast(DoubleType()),
                    col('DHCP').cast(DoubleType()),
                    col('ARP').cast(DoubleType()),
                    col('ICMP').cast(DoubleType()),
                    col('IPv').cast(DoubleType()),
                    col('LLC').cast(DoubleType()),
                    col('Tot sum').cast(DoubleType()),
                    col('Min').cast(DoubleType()),
                    col('Max').cast(DoubleType()),
                    col('AVG').cast(DoubleType()),
                    col('Std').cast(DoubleType()),
                    col('Tot size').cast(DoubleType()),
                    col('IAT').cast(DoubleType()),
                    col('Number').cast(DoubleType()),
                    col('Magnitue').cast(DoubleType()),
                    col('Radius').cast(DoubleType()),
                    col('Covariance').cast(DoubleType()),
                    col('Variance').cast(DoubleType()),
                    col('Weight').cast(DoubleType()),
                    col('label').cast(StringType())))
    return dataset_df


from pyspark.ml import Pipeline, Transformer
from pyspark.ml.feature import StringIndexer
from pyspark import keyword_only
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param
from pyspark.sql.functions import udf,regexp_replace,monotonically_increasing_id
# Dictionary that contains mapping of various attacks to the four main categories
attack_dict = {
    'DDoS-UDP_Flood' : 'DDoS',
    'DDoS-TCP_Flood' : 'DDoS',
    'DDoS-ICMP_Flood' : 'DDoS',
    'DDoS-ACK_Fragmentation' : 'DDoS',
    'DDoS-UDP_Fragmentation' : 'DDoS',
    'DDoS-HTTP_Flood' : 'DDoS',
    'DDoS-SlowLoris' : 'DDoS',
    'DDoS-ICMP_Fragmentation' : 'DDoS',
    'DDoS-PSHACK_Flood' : 'DDoS',
    'DDoS-SynonymousIP_Flood' : 'DDoS',
    'DDoS-RSTFINFlood' : 'DDoS',
    'DDoS-SYN_Flood' : 'DDoS',

    'DoS-UDP_Flood' : 'DoS',
    'DoS-TCP_Flood' : 'DoS',
    'DoS-SYN_Flood' : 'DoS',
    'DoS-HTTP_Flood' : 'DoS',

    'DictionaryBruteForce' : 'Brute Force',


    'XSS' : 'web Based',
    'SqlInjection' : 'web Based',
    'BrowserHijacking' : 'web Based',
    'CommandInjection' : 'web Based',
    'Backdoor_Malware' : 'web Based',
    'Uploading_Attack' : 'web Based',

    'Recon-HostDiscovery' : 'Recon',
    'Recon-OSScan' : 'Recon',
    'Recon-PortScan' : 'Recon',
    'Recon-PingSweep' : 'Recon',
    'VulnerabilityScan' : 'Recon',

    'MITM-ArpSpoofing' : 'Spoofing',
    'DNS_Spoofing' : 'Spoofing',

    'Mirai-greeth_flood' : 'Miria',
    'Mirai-udpplain' : 'Miria',
    'Mirai-greip_flood' : 'Miria',

    'BenignTraffic': 'Benign'
}

attack_mapping_udf = udf(lambda v: attack_dict[v])

class Labels2Converter(Transformer):

    @keyword_only
    def __init__(self):
        super(Labels2Converter, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn('labels2', regexp_replace(col('label'), '^(?!BenignTraffic).*$', 'attack'))
     
class Labels8Converter(Transformer):
    
    @keyword_only
    def __init__(self):
        super(Labels8Converter, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn('labels8', attack_mapping_udf(col('label')))
    
labels2_indexer = StringIndexer(inputCol="labels2", outputCol="labels2_index")
labels8_indexer = StringIndexer(inputCol="labels8", outputCol="labels8_index")

labels_mapping_pipeline = Pipeline(stages=[Labels2Converter(), Labels8Converter(), labels2_indexer, labels8_indexer])

labels2 = ['BenignTraffic', 'attack']
labels8 = ['BenignTraffic', 'DoS', 'DDoS', 'Brute Force', 'web Based','Recon','Spoofing','Miria']
labels_col = 'labels2_index'

# Loading train data
t0 = time()
train_df = load_dataset(train_dataset_path)

# Fitting preparation pipeline
labels_mapping_model = labels_mapping_pipeline.fit(train_df)

# Transforming labels column and adding id column
train_df = labels_mapping_model.transform(train_df).withColumn('id', sql.monotonically_increasing_id())

train_df = train_df
print(f"Number of examples in train set: {train_df.count()}")
print(f"Time: {time() - t0:.2f}s")


# Loading test data
t0 = time()
test_df = load_dataset(test_dataset_path)

# Transforming labels column and adding id column
test_df = labels_mapping_model.transform(test_df).withColumn('id', sql.monotonically_increasing_id())

test_df = test_df
print(f"Number of examples in test set: {test_df.count()}")
print(f"Time: {time() - t0:.2f}s")


# Labels columns
(train_df.groupby('labels2').count().show())
(train_df.groupby('labels8').count().sort(sql.desc('count')).show())


(test_df.groupby('labels2').count().show())
(test_df.groupby('labels8').count().sort(sql.desc('count')).show())


# Numeric columns
print(len(numeric_cols))
(train_df.select(numeric_cols).describe().toPandas().transpose())


train_df = train_df.drop('label')
test_df = test_df.drop('label')
# numeric_cols.remove('num_outbound_cmds')

def ohe_vec(cat_dict, row):
    vec = np.zeros(len(cat_dict))
    vec[cat_dict[row]] = float(1.0)
    return vec.tolist()

def ohe(df, nominal_col):
    categories = (df.select(nominal_col)
                    .distinct()
                    .rdd.map(lambda row: row[0])
                    .collect())
    
    cat_dict = dict(zip(categories, range(len(categories))))
    
    udf_ohe_vec = udf(lambda row: ohe_vec(cat_dict, row), 
                      StructType([StructField(cat, DoubleType(), False) for cat in categories]))
    
    df = df.withColumn(nominal_col + '_ohe', udf_ohe_vec(col(nominal_col)))
    
    nested_cols = [nominal_col + '_ohe.' + cat for cat in categories]
    ohe_cols = [nominal_col + '_' + cat for cat in categories]
        
    for new, old in zip(ohe_cols, nested_cols):
        df = df.withColumn(new, col(old))

    df = df.drop(nominal_col + '_ohe')
                   
    return df, ohe_cols


t0 = time()
train_ohe_cols = []

# train_df, train_ohe_col0 = ohe(train_df, nominal_cols[0])
# train_ohe_cols += train_ohe_col0

# train_df, train_ohe_col1 = ohe(train_df, nominal_cols[1])
# train_ohe_cols += train_ohe_col1

# train_df, train_ohe_col2 = ohe(train_df, nominal_cols[2])
# train_ohe_cols += train_ohe_col2

binary_cols += train_ohe_cols

train_df = train_df
print(f"Number of examples in train set: {train_df.count()}")
print(f"Time: {time() - t0:.2f}s")

t0 = time()
test_ohe_cols = []

# test_df, test_ohe_col0_names = ohe(test_df, nominal_cols[0])
# test_ohe_cols += test_ohe_col0_names

# test_df, test_ohe_col1_names = ohe(test_df, nominal_cols[1])
# test_ohe_cols += test_ohe_col1_names

# test_df, test_ohe_col2_names = ohe(test_df, nominal_cols[2])
# test_ohe_cols += test_ohe_col2_names

test_binary_cols = col_names[binary_inx].tolist() + test_ohe_cols

test_df = test_df
print(f"Number of examples in test set: {test_df.count()}")
print(f"Time: {time() - t0:.2f}s")


def getAttributeRatio(df, numericCols, binaryCols, labelCol):
    ratio_dict = {}
    
    if numericCols:
        avg_dict = (df
                .select(list(map(lambda c: sql.avg(c).alias(c), numericCols)))
                .first()
                .asDict())

        ratio_dict.update(df
                .groupBy(labelCol)
                .avg(*numericCols)
                .select(list(map(lambda c: sql.max(col('avg(' + c + ')')/avg_dict[c]).alias(c), numericCols)))
                .fillna(0.0)
                .first()
                .asDict())
    
    if binaryCols:
        ratio_dict.update((df
                .groupBy(labelCol)
                .agg(*list(map(lambda c: (sql.sum(col(c))/(sql.count(col(c)) - sql.sum(col(c)))).alias(c), binaryCols)))
                .fillna(1000.0)
                .select(*list(map(lambda c: sql.max(col(c)).alias(c), binaryCols)))
                .first()
                .asDict()))
        
    return OrderedDict(sorted(ratio_dict.items(), key=lambda v: -v[1]))

def selectFeaturesByAR(ar_dict, min_ar):
    return [f for f in ar_dict.keys() if ar_dict[f] >= min_ar]

t0 = time()
ar_dict = getAttributeRatio(train_df, numeric_cols, binary_cols, 'labels8')

print(f"Number of features in Attribute Ration dict: {len(ar_dict)}")
print(f"Time: {time() - t0:.2f}s")
ar_dict


t0 = time()
avg_dict = (train_df.select(list(map(lambda c: sql.avg(c).alias(c), numeric_cols))).first().asDict())
std_dict = (train_df.select(list(map(lambda c: sql.stddev(c).alias(c), numeric_cols))).first().asDict())

def standardizer(column):
    return ((col(column) - avg_dict[column])/std_dict[column]).alias(column)

# Standardizer without mean
# def standardizer(column):
#     return (col(column)/std_dict[column]).alias(column)

train_scaler = [*binary_cols, *list(map(standardizer, numeric_cols)), *['id', 'labels2_index', 'labels2', 'labels8_index', 'labels8']]
test_scaler = [*test_binary_cols, *list(map(standardizer, numeric_cols)), *['id', 'labels2_index', 'labels2', 'labels8_index', 'labels8']]

scaled_train_df = (train_df.select(train_scaler))
scaled_test_df = (test_df.select(test_scaler))

print(scaled_train_df.count())
print(scaled_test_df.count())
print(f"Time: {time() - t0:.2f}s")


from pyspark.ml.feature import VectorIndexer, VectorAssembler
assembler = VectorAssembler(inputCols=selectFeaturesByAR(ar_dict, 0.01), outputCol='raw_features')
indexer = VectorIndexer(inputCol='raw_features', outputCol='indexed_features', maxCategories=2)

prep_pipeline = Pipeline(stages=[assembler, indexer])
prep_model = prep_pipeline.fit(scaled_train_df)


t0 = time()
scaled_train_df = (prep_model
        .transform(scaled_train_df)
        .select('id', 'indexed_features', 'labels2_index', 'labels2', 'labels8_index', 'labels8')
        )

scaled_test_df = (prep_model 
        .transform(scaled_test_df)
        .select('id', 'indexed_features','labels2_index', 'labels2', 'labels8_index', 'labels8')
        )

print(scaled_train_df.count())
print(scaled_test_df.count())
print(f"Time: {time() - t0:.2f}s")


# Setting seed for reproducibility
seed = 4667979835606274383
print(seed)


split = (scaled_train_df.randomSplit([0.8, 0.2], seed=seed))

scaled_train_df = split[0]
scaled_cv_df = split[1]

print(scaled_train_df.count())
print(scaled_cv_df.count())

res_cv_df = scaled_cv_df.select(col('id'), col('labels2_index'), col('labels2'), col('labels8'))
res_test_df = scaled_test_df.select(col('id'), col('labels2_index'), col('labels2'), col('labels8'))
prob_cols = []
pred_cols = []

print(res_cv_df.count())
print(res_test_df.count())


import sklearn.metrics as metrics

def printCM(cm, labels):
    """pretty print for confusion matrixes"""
    from builtins import max
    columnwidth = max([len(x) for x in labels])
    # Print header
    # file.write(" " * columnwidth, end="\t")
    print(" " * columnwidth, end="\t")
    for label in labels:
        # file.write("%{0}s".format(columnwidth) % label, end="\t")
        print("%{0}s".format(columnwidth) % label, end="\t")
    print()
    # Print rows
    for i, label1 in enumerate(labels):
        # file.write("%{0}s".format(columnwidth) % label1, end="\t")
        print("%{0}s".format(columnwidth) % label1, end="\t")
        for j in range(len(labels)):
            # file.write("%{0}d".format(columnwidth) % cm[i, j], end="\t")
            print("%{0}d".format(columnwidth) % cm[i, j], end="\t")
        print()

def getPrediction(e):
    return udf(lambda row: 1.0 if row >= e else 0.0, DoubleType())
        
def printReport(resDF, probCol, labelCol='labels2_index', e=None, labels=['normal', 'attack']):
    if (e):
        predictionAndLabels = list(zip(*resDF.rdd
                                       .map(lambda row: (1.0 if row[probCol] >= e else 0.0, row[labelCol]))
                                       .collect()))
    else:
        predictionAndLabels = list(zip(*resDF.rdd
                                       .map(lambda row: (row[probCol], row[labelCol]))
                                       .collect()))
    
    cm = metrics.confusion_matrix(predictionAndLabels[1], predictionAndLabels[0])
    printCM(cm, labels)
    # file.write(" ")
    print(" ")
    # file.write("Accuracy = %g" % (metrics.accuracy_score(predictionAndLabels[1], predictionAndLabels[0])))
    print("Accuracy = %g" % (metrics.accuracy_score(predictionAndLabels[1], predictionAndLabels[0])))
    # file.write("AUC = %g" % (metrics.roc_auc_score(predictionAndLabels[1], predictionAndLabels[0])))
    print("AUC = %g" % (metrics.roc_auc_score(predictionAndLabels[1], predictionAndLabels[0])))
    # file.write(" ")
    print(" ")
    # file.write("False Alarm Rate = %g" % (cm[0][1]/(cm[0][0] + cm[0][1])))
    print("False Alarm Rate = %g" % (cm[0][1]/(cm[0][0] + cm[0][1])))
    # file.write("Detection Rate = %g" % (cm[1][1]/(cm[1][1] + cm[1][0])))
    print("Detection Rate = %g" % (cm[1][1]/(cm[1][1] + cm[1][0])))
    f1_score = metrics.f1_score(predictionAndLabels[1], predictionAndLabels[0], labels=labels)
    # file.write(f"F1 score = {f1_score}")
    print(f"F1 score = {f1_score}")
    # file.write(" ")
    print(" ")
    # file.write(metrics.classification_report(predictionAndLabels[1], predictionAndLabels[0]))
    print(metrics.classification_report(predictionAndLabels[1], predictionAndLabels[0]))
    # file.write(" ")
    print(" ")

from pyspark.ml.feature import VectorSlicer
from pyspark.ml.feature import PCA

t0 = time()
pca_slicer = VectorSlicer(inputCol="indexed_features", outputCol="features", names=selectFeaturesByAR(ar_dict, 0.05))

pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_pipeline = Pipeline(stages=[pca_slicer, pca])

pca_train_df = pca_pipeline.fit(scaled_train_df).transform(scaled_train_df)
print(f"Time: {time() - t0:.2f}s")

kmeans_prob_col = 'kmeans_rf_prob'
kmeans_pred_col = 'kmeans_rf_pred'

prob_cols.append(kmeans_prob_col)
pred_cols.append(kmeans_pred_col)


# KMeans clustrering
from pyspark.ml.clustering import KMeans
t1 = time()
t0 = time()
kmeans_slicer = VectorSlicer(inputCol="indexed_features", outputCol="features", 
                             names=list(set(selectFeaturesByAR(ar_dict, 0.1)).intersection(numeric_cols)))

kmeans = KMeans(k=8, initSteps=25, maxIter=100, featuresCol="features", predictionCol="cluster", seed=seed)

kmeans_pipeline = Pipeline(stages=[kmeans_slicer, kmeans])

kmeans_model = kmeans_pipeline.fit(scaled_train_df)

kmeans_train_df = kmeans_model.transform(scaled_train_df)
kmeans_cv_df = kmeans_model.transform(scaled_cv_df)
kmeans_test_df = kmeans_model.transform(scaled_test_df)
# file.write(" ")
print(f"Time: {time() - t0:.2f}s")


# Function for describing the contents of the clusters 
def getClusterCrosstab(df, clusterCol='cluster'):
    return (df.crosstab(clusterCol, 'labels2')
              .withColumn('count', col('attack') + col('BenignTraffic'))
              .withColumn(clusterCol + '_labels2', col(clusterCol + '_labels2').cast('int'))
              .sort(col(clusterCol +'_labels2').asc()))

kmeans_crosstab = getClusterCrosstab(kmeans_train_df)
kmeans_crosstab.show(n=30)

# Function for splitting clusters
def splitClusters(crosstab):
    exp = ((col('count') > 25) & (col('attack') > 0) & (col('BenignTraffic') > 0))

    cluster_rf = (crosstab
        .filter(exp).rdd
        .map(lambda row: (int(row['cluster_labels2']), [row['count'], row['attack']/row['count']]))
        .collectAsMap())

    cluster_mapping = (crosstab
        .filter(~exp).rdd
        .map(lambda row: (int(row['cluster_labels2']), 1.0 if (row['count'] <= 25) | (row['BenignTraffic'] == 0) else 0.0))
        .collectAsMap())
    
    return cluster_rf, cluster_mapping

kmeans_cluster_rf, kmeans_cluster_mapping = splitClusters(kmeans_crosstab)

print(len(kmeans_cluster_rf), len(kmeans_cluster_mapping))
print(kmeans_cluster_mapping)
kmeans_cluster_rf



from pyspark.ml.classification import RandomForestClassifier,LinearSVC,DecisionTreeClassifier,FMClassifier,GBTClassifier

# This function returns Random Forest models for provided clusters
# def getClusterModels(df, cluster_rf):
#     cluster_models = {}

#     labels_col = 'labels2_cl_index'
#     labels2_indexer.setOutputCol(labels_col)

#     rf_slicer = VectorSlicer(inputCol="indexed_features", outputCol="rf_features", 
#                              names=selectFeaturesByAR(ar_dict, 0.05))

#     for cluster in cluster_rf.keys():
#         t1 = time()
#         rf_classifier = RandomForestClassifier(labelCol=labels_col, featuresCol='rf_features', seed=seed,
#                                                numTrees=500, maxDepth=20, featureSubsetStrategy="sqrt")
        
#         rf_pipeline = Pipeline(stages=[labels2_indexer, rf_slicer, rf_classifier])
#         cluster_models[cluster] = rf_pipeline.fit(df.filter(col('cluster') == cluster))
#         print("Finished %g cluster in %g s" % (cluster, time() - t1))
        
#     return cluster_models

# def getClusterModels(df, cluster_rf):
#     cluster_models = {}

#     labels_col = 'labels2_cl_index'
#     labels2_indexer.setOutputCol(labels_col)

#     rf_slicer = VectorSlicer(inputCol="indexed_features", outputCol="svm_features",
#                              names=selectFeaturesByAR(ar_dict, 0.05))

#     for cluster in cluster_rf.keys():
#         t1 = time()
#         svm_classifier = LinearSVC(labelCol=labels_col, featuresCol='svm_features', maxIter=100, regParam=0.1)

#         svm_pipeline = Pipeline(
#             stages=[labels2_indexer, rf_slicer, svm_classifier])
#         cluster_models[cluster] = svm_pipeline.fit(
#             df.filter(col('cluster') == cluster))
#         print("Finished %g cluster in %g s" % (cluster, time() - t1))

#     return cluster_models



def getClusterModels(df, cluster_rf):
    cluster_models = {}

    labels_col = 'labels2_cl_index'
    labels2_indexer.setOutputCol(labels_col)

    rf_slicer = VectorSlicer(inputCol="indexed_features", outputCol="dt_features",
                             names=selectFeaturesByAR(ar_dict, 0.05))

    for cluster in cluster_rf.keys():
        t1 = time()
        dt_classifier = DecisionTreeClassifier(labelCol=labels_col, featuresCol='dt_features', seed=seed,
                                               maxDepth=20)

        dt_pipeline = Pipeline(
            stages=[labels2_indexer, rf_slicer, dt_classifier])
        cluster_models[cluster] = dt_pipeline.fit(
            df.filter(col('cluster') == cluster))
        print("Finished %g cluster in %g s" % (cluster, time() - t1))

    return cluster_models

# def getClusterModels(df, cluster_rf):
#     cluster_models = {}

#     labels_col = 'labels2_cl_index'
#     labels2_indexer.setOutputCol(labels_col)

#     rf_slicer = VectorSlicer(inputCol="indexed_features", outputCol="rf_features",
#                              names=selectFeaturesByAR(ar_dict, 0.05))

#     for cluster in cluster_rf.keys():
#         t1 = time()

#         # FMClassifier configuration
#         fm_classifier = FMClassifier(labelCol=labels_col, featuresCol='rf_features', stepSize=0.03, seed=seed)

#         rf_pipeline = Pipeline(
#             stages=[labels2_indexer, rf_slicer, fm_classifier])

#         cluster_models[cluster] = rf_pipeline.fit(
#             df.filter(col('cluster') == cluster))
#         print("Finished %g cluster in %g s" % (cluster, time() - t1))

#     return cluster_models

# def getClusterModels(df, cluster_rf):
#     cluster_models = {}
    
#     labels_col = 'labels2_cl_index'
#     labels2_indexer = StringIndexer(inputCol="labels2", outputCol=labels_col)

#     rf_slicer = VectorSlicer(inputCol="indexed_features", outputCol="rf_features",
#                              names=selectFeaturesByAR(ar_dict, 0.05))

#     for cluster in cluster_rf.keys():
#         t1 = time()
#         gbt_classifier = GBTClassifier(labelCol='gbt_labels', featuresCol='rf_features', maxIter=10, maxDepth=5)

#         rf_pipeline = Pipeline(
#             stages=[labels2_indexer, rf_slicer, gbt_classifier])
        
#         model = rf_pipeline.fit(df.filter(col('cluster') == cluster))
#         cluster_models[cluster] = model
        
#         print("Finished %g cluster in %g s" % (cluster, time() - t1))

#     return cluster_models
# This utility function helps to get predictions/probabilities for the new data and return them into one dataframe
# def getProbabilities(df, probCol, cluster_mapping, cluster_models):
#     pred_df = (sqlContext.createDataFrame([], StructType([
#                     StructField('id', LongType(), False),
#                     StructField(probCol, DoubleType(), False)])))
    
#     udf_map = udf(lambda cluster: cluster_mapping[cluster], DoubleType())
#     pred_df = pred_df.union(df.filter(col('cluster').isin(list(cluster_mapping.keys())))
#                             .withColumn(probCol, udf_map(col('cluster')))
#                             .select('id', probCol))

                                       
#     for k in cluster_models.keys():
#         maj_label = cluster_models[k].stages[0].labels[0]
#         udf_remap_prob = udf(lambda row: float(row[0]) if (maj_label == 'attack') else float(row[1]), DoubleType())

#         pred_df = pred_df.union(cluster_models[k]
#                          .transform(df.filter(col('cluster') == k))
#                          .withColumn(probCol, udf_remap_prob(col('prediction')))
#                          .select('id', probCol))

#     return pred_df
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, LongType, DoubleType
from pyspark.ml.linalg import DenseVector

def getProbabilities(df, probCol, cluster_mapping, cluster_models):
    # Define the schema for the resulting DataFrame
    schema = StructType([
        StructField('id', LongType(), False),
        StructField(probCol, DoubleType(), False)
    ])

    # Create an empty DataFrame with the defined schema
    spark = SparkSession.builder.getOrCreate()
    pred_df = spark.createDataFrame([], schema)
    
    # UDF to map cluster values based on the cluster_mapping
    udf_map = udf(lambda cluster: cluster_mapping[cluster], DoubleType())
    
    # Add initial cluster mapping probabilities to pred_df
    initial_pred_df = df.filter(col('cluster').isin(list(cluster_mapping.keys()))) \
                        .withColumn(probCol, udf_map(col('cluster'))) \
                        .select('id', probCol)
    pred_df = pred_df.union(initial_pred_df)

    # UDF to extract the probability of the positive class from the probability vector
    def extract_prob(prob):
        if isinstance(prob, DenseVector):
            return float(prob[1])
        else:
            return float(prob[1]) if isinstance(prob, (list, tuple)) else prob
    
    udf_extract_prob = udf(extract_prob, DoubleType())
    
    # Process each cluster model and add the transformed data to pred_df
    for k in cluster_models.keys():
        cluster_pred_df = cluster_models[k] \
            .transform(df.filter(col('cluster') == k)) \
            .withColumn(probCol, udf_extract_prob(col('probability'))) \
            .select('id', probCol)
        
        pred_df = pred_df.union(cluster_pred_df)

    return pred_df


# def getProbabilities(df, prob_col_name, cluster_mapping, cluster_models):
#     def remap_prob(raw_pred):
#         # Example remapping function; adjust as needed
#         return float(raw_pred[1]) if raw_pred[1] > 0 else 1 / (1 + abs(float(raw_pred[1])))

#     udf_remap_prob = udf(remap_prob, DoubleType())

#     for cluster, model in cluster_models.items():
#         df = df.filter(col('cluster') == cluster)
#         df = model.transform(df)
#         df = df.withColumn(prob_col_name, udf_remap_prob(col('rawPrediction')))
        
#     return df



# Training Random Forest classifiers for each of the clusters
t0 = time()
kmeans_cluster_models = getClusterModels(kmeans_train_df, kmeans_cluster_rf)
print(f"Time: {time() - t0:.2f}s")


# Getting probabilities for CV data
t0 = time()
res_cv_df = (res_cv_df.drop(kmeans_prob_col)
             .join(getProbabilities(kmeans_cv_df, kmeans_prob_col, kmeans_cluster_mapping, kmeans_cluster_models), 'id')
             )

print(res_cv_df.count())
print(f"Time: {time() - t0:.2f}s")

# Getting probabilities for Test data
t0 = time()
res_test_df = (res_test_df.drop(kmeans_prob_col)
               .join(getProbabilities(kmeans_test_df, kmeans_prob_col, kmeans_cluster_mapping, kmeans_cluster_models), 'id')
               )

print(res_test_df.count())
print(f"Time: {time() - t0:.2f}s")


printReport(res_cv_df, kmeans_prob_col, e=0.5, labels=labels2)

printReport(res_test_df, kmeans_prob_col, e=0.01, labels=labels2)

# file.write(" ")
print(f"Train Time: {time() - t1:.2f}s")
# # Adding prediction columns based on chosen thresholds into result dataframes
# t0 = time()
# res_cv_df = res_cv_df.withColumn(kmeans_pred_col, getPrediction(0.5)(col(kmeans_prob_col)))
# res_test_df = res_test_df.withColumn(kmeans_pred_col, getPrediction(0.01)(col(kmeans_prob_col)))

# print(res_cv_df.count())
# print(res_test_df.count())
# print(f"Time: {time() - t0:.2f}s")
