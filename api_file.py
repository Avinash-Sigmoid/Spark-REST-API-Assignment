# Give Proper variable name.
#instead of x and y proper variable names can be given for better understanding.
#Add comments where ever possible.


from flask import Flask,jsonify,json
from pyspark.sql import SparkSession
from flask import Response


app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False



spark = SparkSession.builder.appName(
  'Read All CSV Files in Directory').getOrCreate()

file2 = spark.read.csv('/Users/avinashkumar/PycharmProjects/StockDataAnalysis/*.csv', sep=',',
          inferSchema=True, header=True)

#q9
file2.createOrReplaceTempView("data")

@app.route('/highest_and_lowest_prices_for_each_stock',methods=['GET'])
def get_highest_and_lowest_prices_for_each_stock():
    x=spark.sql("select CompanyName,MAX(High) as Highest_Price,MIN(Low) as Lowest_Price from data group by CompanyName  ")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results,200)
#Q8
@app.route('/stock_has_higher_average_volume',methods=['GET'])
def get_stock_has_higher_average_volume():
    x=spark.sql(
        "select CompanyName,AVG(Volume) as avgvolume from data group by CompanyName order by avgvolume DESC limit 1 ")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q7
@app.route('/average_volume_of_stocks',methods=['GET'])
def get_average_volume_of_stocks():
    x=spark.sql("select AVG(Volume) as avgvolume from data  ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q6
@app.route('/mean_and_median_of_each_stock',methods=['GET'])
def get_mean_and_median_of_each_stock():
    x=spark.sql("select CompanyName,AVG(Open) from data group by CompanyName ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    y=spark.sql(
        "select distinct * from (select CompanyName,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY Open) OVER (PARTITION BY CompanyName) AS Median_UnitPrice from data)")
    results1 = y.toJSON().map(lambda j: json.loads(j)).collect()

    final={}
    final["mean"]=results
    final["median"]=results1

    return jsonify(final, 200)
#Q5
@app.route('/standard_deviation_of_each_stock',methods=['GET'])
def get_standard_deviation_of_each_stock():
    x=spark.sql("select CompanyName,STDDEV(Open) as STD_OpenPrice from data group by CompanyName ")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q4
@app.route('/stock_moved_maximum',methods=['GET'])
def get_stock_moved_maximum():
    x=spark.sql("select CompanyName,Open,High,(High-Open) as max_diff from (Select CompanyName, (Select Open from data limit 1) as Open, max(High) as High from data group by CompanyName)data order by max_diff desc limit 1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q3
@app.route('/max_gap_moved',methods=['GET'])
def get_max_gap_moved():
    x=spark.sql("with added_previous_close as (select CompanyName,OPen,Date,Close,LAG(Close,1,35.724998) over(partition by CompanyName order by Date) as previous_close from data ASC) select CompanyName,ABS(previous_close-Open) as max_swing from added_previous_close order by max_swing DESC LIMIT 1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q2
@app.route('/most_traded_stock',methods=['GET'])
def get_most_traded_stock():
    x=spark.sql("WITH added_dense_rank AS (SELECT Date,CompanyName,Volume , dense_rank() OVER ( partition by Date order by Volume desc ) as dense_rank FROM data) select Date,CompanyName,Volume FROM added_dense_rank where dense_rank=1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)
#Q1
# @app.route('/stock_moved_up_highest',methods=['GET'])
# def get_stock_moved_up_highest():
#     x=spark.sql("WITH added_dense_rank AS (SELECT Date,CompanyName,(High-Open)/Open as Up_Percentage, dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM data) select Date,CompanyName,Up_Percentage FROM added_dense_rank where dense_rank=1")
#     results = x.toJSON().map(lambda j: json.loads(j)).collect()
#     return jsonify(results, 200)
# @app.route('/stock_moved_down_lowest',methods=['GET'])
# def get_stock_moved_down_lowest():
#     x=spark.sql("WITH added_dense_rank AS (SELECT Date,CompanyName,(Open-Low)/Open as Down_Percentage, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM data) select Date,CompanyName,Down_Percentage FROM added_dense_rank where dense_rank=1")
#     results = x.toJSON().map(lambda j: json.loads(j)).collect()
#     return jsonify(results, 200)
#Q1
@app.route('/stock_moved_up_or_down', methods=['GET'])
def get_stock_moved_up_or_down():
    stock_moved_up_highest = spark.sql(
        "WITH added_dense_rank AS (SELECT Date,CompanyName,(High-Open)/Open as up_rcentage, dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM data) select Date,CompanyName,up_rcentage FROM added_dense_rank where dense_rank=1")
    stock_moved_up_lowest = spark.sql(
        "WITH added_dense_rank AS (SELECT Date,CompanyName,(Open-Low)/Open as Down_Percentage, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM data) select Date,CompanyName,Down_Percentage FROM added_dense_rank where dense_rank=1")

    stock_moved_up_lowest = stock_moved_up_lowest.withColumnRenamed("CompanyName", "maxdown_company")
    stock_moved_up_highest = stock_moved_up_highest.withColumnRenamed("CompanyName", "maxup_company")

    output = stock_moved_up_highest.join(stock_moved_up_lowest, ['Date'], how='inner')
    output.show(20, False)
    final = output.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(final, 200)







app.run(host='0.0.0.0',port=5008)
