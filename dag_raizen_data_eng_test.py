from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, DoubleType
from pyspark.sql.functions import lit, unix_timestamp
import pandas as pd
import urllib.request
import time
import random
import datetime

# This Airflow DAG is part of the Raizen Analytics Data Engineering Test proposed solution by AlbertoTav
# More info at https://github.com/albertotav/raizen-data-engineering-test

class Dataset:
    def __init__(self):
        '''
        Returns the ANP dataset for the Data Engineering Test by Raizen Analytics.
        '''  
        self.df_raw = self._build_df()
        self.pandas_df = self._translate_setdate()
        self.spark = self._start_spark_session()
        self.spark_df = self._spark_create_df(self.pandas_df)
        self.passed = self.verify()


    def _git_source(self):
        '''
        Stores github link to raw datasets and filepaths
        '''
        url1 = 'https://github.com/albertotav/raizen-data-engineering-test/blob/main/raw_data/diesel_by_uf_and_type.csv.gz?raw=true'
        path1= '/home/alberto/data/diesel_by_uf_and_type.csv.gz'
        url2 = 'https://github.com/albertotav/raizen-data-engineering-test/blob/main/raw_data/oil_derivative_fuels_by_uf_and_product.csv.gz?raw=true'
        path2= '/home/alberto/data/oil_derivative_fuels_by_uf_and_product.csv.gz'
        return url1, path1, url2, path2

    def _get_raw_csv(self, url, path):
        '''
        Request file from url
        '''
        urllib.request.urlretrieve(url, path)

    def _read_csv(self, path,compression='gzip', encoding='iso-8859-1'):
        '''
        Read .csv.gz with Latin alphabet encoding
        '''
        df = pd.read_csv(path,compression=compression, encoding=encoding)
        return df

    def _concat_df(self, url1, path1, url2, path2):
        '''
        Concat both pivot tables dataset
        '''
        self._get_raw_csv(url1, path1)
        self._get_raw_csv(url2, path2)
        df1 = self._read_csv(path1)
        df2 = self._read_csv(path2)
        df = pd.concat([df1, df2], axis=0)
        return df

    def _build_df(self):
        url1, path1, url2, path2 = self._git_source()
        df = self._concat_df(url1, path1, url2, path2)
        return df

    def _translate_setdate(self):
        """
        Translate dataframe colums names to english and creates year_month datetype column.

        :param pd.DataFrame df: diesel-by-uf-and-type.csv or oil-derivative-fuels-by-uf-and-product.csv derivate Pandas dataframe

        :returns: DataFrame with processed data.
        :rtype: pd.DataFrame
        """
        df = self.df_raw.copy(deep=True)
        df.columns = ['product','year','region','uf','unit', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec','TOTAL']

        df = pd.melt(df, 
            id_vars=list(df.columns[0:5]), 
            value_vars=list(df.columns[5:-1]),
            var_name='month',
            value_name='volume')

        df['year_month'] = df['year'].astype(str)+'-'+df['month']
        df['year_month'] = pd.to_datetime(df['year_month'], format= '%Y-%b').dt.date

        df = df[['year_month','uf','product','unit','volume']]
        df['volume'].fillna(0, inplace=True) # Fills volume column NaNs with zero. Those are present in the months of 2020 not yet accounted for.
        return df

    def _start_spark_session(self):
        '''
        Returns a spark session
        '''
        spark = SparkSession.builder \
        .appName('Python-spark-session') \
        .config('spark.sql.session.timeZone', 'America/Sao_Paulo') \
        .getOrCreate()
        return spark

    def _spark_create_df(self, df):
        '''
        Build the dataset as a spark dataframe with the Test requested schema
        '''        
        # Defines schema as requested by the challenge
        schema = StructType([
            StructField('year_month', DateType(), False),
            StructField('uf', StringType(), False),
            StructField('product', StringType(), False),
            StructField('unit', StringType(), False),
            StructField('volume', DoubleType(), True),
            ])

        # Creates Spark Dataframes with required schema and timestamp column
        spark_df = self.spark.createDataFrame(df, schema=schema) \
        .withColumn('created_at', unix_timestamp(lit(datetime.datetime.fromtimestamp(time.time()) \
        .strftime('%Y-%m-%d %H:%M:%S')),'yyyy-MM-dd HH:mm:ss') \
        .cast("timestamp")) 
        return spark_df


    def _rnd_query(self, df_raw, table_name='temp'):
        """
        Creates a random query to extract the sum of the column volume that matches a random year, product and UF. 
        Also return the matching total column value from the original raw data.

        :param pd.DataFrame df: diesel-by-uf-and-type.csv or oil-derivative-fuels-by-uf-and-product.csv derivate Pandas dataframe
        :param string table_name: Name of the refined Spark dataframe derived table from raw data

        :returns: SUM Query for random parameters with expected total column value from raw data
        """

        # Select random values for UF, Product and Year with corresponding Total value from Pandas raw Dataframe
        rnd_uf = str(random.choice(df_raw.iloc[:,3].unique()))
        rnd_product = str(random.choice(df_raw.iloc[:,0].unique()))
        rnd_year = random.choice(df_raw.iloc[:,1].unique())

        error = True
        while error: # Check if the total value exist for rnd_product
            try:
                rnd_total = df_raw.loc[(df_raw.iloc[:,1]==rnd_year) \
                    & (df_raw.iloc[:,3]==rnd_uf) \
                    & (df_raw.iloc[:,0]==rnd_product)].iloc[0,-1]
                error = False
            except:
                rnd_product = str(random.choice(df_raw.iloc[:,0].unique()))            

        query = ("SELECT SUM(volume) FROM {table_name} \
                WHERE uf == '{rnd_uf}' AND \
                YEAR(year_month) == {rnd_year} AND \
                product == '{rnd_product}'").format(table_name=table_name, rnd_uf=rnd_uf, rnd_year=rnd_year, rnd_product=rnd_product)
        return query, rnd_total

    def verify(self, table_name='temp'):
        """
        Verifies if the SUM of volume column from table using random parameters matches the corresponding total in the original data.
        Raise error if it doesn't.

        :param pd.DataFrame df: diesel-by-uf-and-type.csv or oil-derivative-fuels-by-uf-and-product.csv derivate Pandas dataframe
        :param string table_name: Name of the refined Spark dataframe derived table from raw data
        """
        query, rnd_total = self._rnd_query(self.df_raw, table_name)
        self.spark_df.createOrReplaceTempView(table_name)
        results = self.spark.sql(query)

        if abs(results.head()[0] - rnd_total) > 0.01: # Discard differences of small decimal digits
            raise ValueError('Verify data consistency')        
        return True

    def _to_parquet(self, path, partition_by, mode):
        '''
        Write dataset as parquet
        '''
        if partition_by:
            self.spark_df.write.partitionBy(partition_by).mode(mode).parquet(path)
        else:
            self.spark_df.write.mode(mode).parquet(path)

    def save(self, path, partition_by=None, mode='append', _format='parquet'):
        '''
        Save dataset
        '''
        formats = {
            'parquet': self._to_parquet,
        }
        save_as = formats[_format]
        save_as(path, partition_by, mode)

def parse(path):
    dataset = Dataset()
    dataset.save(path)

args = {
    'owner': 'AlbertoTav',
}

dag = DAG(
    dag_id='raizen_eng_test',
    start_date=datetime.datetime(2021,6,24,21,0,0),
    schedule_interval='@once',
    default_args=args
)

parse_data = PythonOperator(
    task_id='parse_date',
    python_callable=parse,
    op_kwargs={
        'path': '/home/alberto/data/oil_and_diesel_refined_data_parquet',
    },
    dag=dag
)

parse_data