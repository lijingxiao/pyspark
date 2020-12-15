
import re
import sys
import os
import pandas as pd
import numpy as np
import datetime
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,LongType,FloatType,DoubleType,StringType,ArrayType
from pyspark.sql.window import Window
from ..base import logger,logstr,move_list,flatten,get_rand_tm,get_hdfs_files,mkdir_folder,create_spark,list2str
from ._udf import cast2str_udf,get_addlist_Column,get_addrand_Column



__all__=['sdf_shuffle',
         'sdf_union',
         'sdf_process_duplicate_cols',
         'sdf_percentile',
         'sdf_correlation',
         'get_df_by_parquet',
         'sdf_to_pandas',
         'df_to_hdfs_parquet',
         'pandas_to_sdf',
         'collect_orderby',
         'sdf_pooling_sequence',
         'sdf_join_by_dataskew',
         'sdf_join_by_dataskew_quantile',
         'mapping_sequence']

##### pyspark dataframe分析与转换类
# pyspark dataframe shuffle函数
def sdf_shuffle(sdf, seed=123):
    """
    Paramter:
    ----------
    sdf: pyspark dataframe
    seed: int of the random seed
    Return:
    ----------
    sdf: pyspark dataframe
    """
    sdf = sdf.sort(F.rand(seed)) # sort升序时，null在最前
    return sdf

def sdf_union(*sdf_list):
    if isinstance(sdf_list[0],list):
        sdf_list=sdf_list[0]
    for i,d in enumerate(sdf_list):
        if i==0:
            sdf=d
            continue
        else:
            sdf=sdf.union(d)
    return sdf

def sdf_process_duplicate_cols(sdf,drop=True,sep='__'):
    from collections import Counter
    cols=sdf.columns
    col_counts_dict={k:v for k,v in Counter(cols).items() if v>1}
    col_counts_dict_=col_counts_dict.copy()
    drop_cols=[]
    for i,c in enumerate(cols):
        if c in col_counts_dict_:
            index=col_counts_dict[c]-col_counts_dict_[c]
            new_c=f"{c}{sep}{index}"
            if not drop or index!=0:
                cols[i]=new_c
                if drop and index!=0:
                    drop_cols.append(new_c)
            col_counts_dict_[c]-=1
    sdf=sdf.toDF(*cols).drop(*drop_cols)
    return sdf

def sdf_percentile(sdf, percentile=[0, 0.01, 0.1, 0.3, 0.5, 0.6, 0.7, 0.8,0.85, 0.9, 0.95, 0.99, 1.0], cols=None,approx_flag=False):
    percent_fun="percentile_approx" if approx_flag else "percentile"
    if not isinstance(percentile,list):
        percentile=[percentile]
    def percentile_sql(x): return f"{percent_fun}({x},array(%s)) {x}" % list2str(
        percentile, comma=False)
    cols = sdf.columns if not cols else cols
    if not isinstance(cols,list):
        cols=[cols]
    cols_=[]
    for c in cols:
        if '(' in c or ')' in c:
            res=re.findall(re.compile(r'(.*)\((.*)\)'),c)
            if len(res)>0:
                c_now='_'.join(list(res[0])[::-1])
            else:
                c_now=re.sub(r"[(|)]", "",c)
            sdf=sdf.withColumnRenamed(c,c_now)    
            cols_.append(c_now)
        else:
            cols_.append(c)
    sql_list = [percentile_sql(x) for x in cols_]
    df = sdf.selectExpr(*sql_list).toPandas()
    df_dict = dict(zip(df.keys(), df.values.flatten().tolist()))
    df_dict = {k: dict(zip(percentile, v)) for k, v in df_dict.items()}
    return df_dict


def sdf_correlation(sdf, cols1=None, cols2=None):
    if cols1 is None or cols2 is None:
        if cols1 is None and cols2 is None:
            cols1 = sdf.columns
        elif cols1 is None:
            cols1 = cols2
        cols2 = []
    if not isinstance(cols1, list):
        cols1 = [cols1]
    if not isinstance(cols1, list):
        cols2 = [cols2]
    if len(cols2) == 0:
        all_combinations = [i for i in itertools.combinations(cols1, 2)]
    else:
        all_combinations = [(i, j) for i in cols1 for j in cols2 if i != j]
    assert len(cols1+cols2) > 1
    agg_func_list = [F.corr(*x).alias('_'.join(x)+'__corr')
                     for x in all_combinations]
    sdf = sdf.agg(*agg_func_list)
    return sdf
# import pyarrow as pa
# import pyarrow.parquet as pq

#files为本地或hdfs文件都可以
def get_df_by_parquet(files):
    import pyarrow.parquet as pq
    if not isinstance(files,list):
        files=[files]
    logstr(f"共{len(files)}个parquet文件")
    try:
        df=pd.concat([pq.read_table(f).to_pandas() for f in files],ignore_index=True)
    except Exception as e:
#         logstr("Exception",e)
        df=pd.concat([pd.read_parquet(f) for f in files],ignore_index=True)
    return df

def sdf_to_pandas(sdf,repartion=50,key_name=None,save_hdfs_head="hdfs://ns1/user/mart_dos/pcc/parquet",spark=None,**k):
    if spark is None:
        spark=create_spark()
    if key_name is None:
        save_hdfs=f"{save_hdfs_head}/{get_rand_tm(2)}"
    else:
        save_hdfs=f"{save_hdfs_head}/{key_name}"
    if len(k) != 0:
        save_hdfs = "%s/%s" % (save_hdfs, '/'.join(
            ['%s=%s' % (str(x).strip(), str(y).strip()) for x, y in k.items()]))
    logstr("save_hdfs",save_hdfs)
    mkdir_folder(save_hdfs,type='folder',local_flag=False)
    sdf.repartition(repartion).write.format("parquet").mode("overwrite").save(save_hdfs)
    hdfs_list=get_hdfs_files(save_hdfs)
    df=get_df_by_parquet(hdfs_list)
    return df

#仅单个df，到单个hdfs(仅能overwrite)
def df_to_hdfs_parquet(df,key_name=None,save_hdfs_head="hdfs://ns1/user/mart_dos/pcc/parquet",first_flag=True,**k):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if key_name is None:
        save_hdfs=f"{save_hdfs_head}/{get_rand_tm(2)}"
    else:
        save_hdfs=f"{save_hdfs_head}/{key_name}"
    if len(k) != 0:
        save_hdfs = "%s/%s" % (save_hdfs, '/'.join(
            ['%s=%s' % (str(x).strip(), str(y).strip()) for x, y in k.items()]))
    if first_flag:
        logstr("save_hdfs",save_hdfs)
        mkdir_folder(save_hdfs,type='files',local_flag=False)
    pq.write_table(pa.Table.from_pandas(df),save_hdfs)
    return save_hdfs

def pandas_to_sdf(df,partition=None,key_name=None,save_hdfs_head="hdfs://ns1/user/mart_dos/pcc/parquet",spark=None,**k):
    if spark is None:
        spark=create_spark()
    hdfs_list=[]
    if partition is None and len(df)>=2000000:
        partition=int(np.ceil(len(df)/2000000))
    logstr('partition',partition)
    if partition is None:
        hdfs_list.append(df_to_hdfs_parquet(df,key_name,save_hdfs_head,**k))
    else:
        if key_name is None:
            key_name=get_rand_tm(2)
        partition_index_list=np.linspace(0,len(df),partition+1).astype(int)
        for i,x in enumerate(partition_index_list):
            if i==0:
                continue
            hdfs_list.append(df_to_hdfs_parquet(df.iloc[partition_index_list[i-1]:x,:],\
                                                key_name,save_hdfs_head,first_flag=True,**k,n=i))
    sdf=spark.read.parquet(*hdfs_list)
    return sdf

##### pyspark dataframe操作类

def collect_orderby(sdf, cols, groupby, orderby=None, suffix='__collect', sep='____', orderby_func={}, dtype=StringType(),ascending=True,drop_null=True):
    # 暂时不考虑空值填充,orderby=None时将去除空值
    """
    Paramter:
    ----------
    sdf: pyspark dataframe to be processed
    cols: str/list of the sdf'cols to be processed
    groupby: str/list of sdf' cols to be groupbyed when collect_orderby
    orderby: str/list of sdf' cols to be orderbyed when collect_orderby
    suffix: str of cols' names converted bycollect_orderby(renamed by cols+suffix)
    sep: str of the sep when concat_ws(don't change by default)
    dtype: pyspark.sql.types of the return values
    Return:
    ----------
    sdf: pyspark dataframe of collect_list orderby
    Example:
    ----------
    sdf=collect_orderby(sdf,cols,groupby='user_id',orderby='time')
    """
    # cols:需collect_list项
    # groupby:为空时可传入[]
    # orderby:必为string、int、float项（也可有int,float型）
    assert not orderby_func or orderby
    orderby_agg_func = []
    orderby_agg_cols = []
    orderby_copy_cols_dict,orderby_recover_cols_dict={},{}# 用于orderby中有非string字段进行collect时的名称统一
    if not isinstance(cols, list):
        cols = [cols]
    if not isinstance(groupby, list):
        groupby = [groupby]
    if orderby is None:
        orderby=[]
        orderby_func={}
    if not isinstance(orderby, list):
        orderby = [orderby]
     # 如果orderby有字段也要进行collect且是非string类型时，需要做一次字段复制，否则会将1变成'01'
    for i,c in enumerate(orderby): 
        if c in cols and dict(sdf.select(orderby).dtypes)[c]!='string':
            c_orderby=f"{c}{sep}orderby"
            sdf=sdf.withColumn(c_orderby,F.col(c))
            orderby[i]=c_orderby
            orderby_copy_cols_dict[c_orderby]=c
    if not isinstance(orderby_func, dict):
        if not isinstance(orderby_func, list):
            orderby_func=[orderby_func]
        orderby_func = dict(zip(orderby, [orderby_func]*len(orderby)))
    if not drop_null:
        split_udf = F.udf(lambda x: [i.split(sep)[-1] if len(i.split(sep))>1 else None #当原始字段包含sep时，这里将有问题！！！！
                                 for i in x], ArrayType(dtype))
    else:
        split_udf = F.udf(lambda x: [i.split(sep)[-1] #当原始字段包含sep时，这里将有问题！！！！
                         for i in x if len(i.split(sep))>1], ArrayType(dtype))
    for c in [k for k, v in sdf.dtypes if k in cols and len(re.findall(re.compile(r'^(array|vector)'), v)) > 0]:
        logstr(f'{c}类型转换为StringType')
        sdf = sdf.withColumn(c,cast2str_udf(c))  # 不符合要求的先统计转为StringType()
    logstr('orderby',orderby)
    if len(orderby)!=0:
        # 处理orderby_func
        for c, f_list in orderby_func.items():
            if not isinstance(f_list, list):
                f_list = [f_list]
            for i, f in enumerate(f_list):
                if c not in orderby:
                    continue
                if isinstance(f, str):
                    f = f_list[i] = eval(f"F.{f}")
                key = f"{c}{sep}{f.__name__}"
                orderby_agg_func.append(f(c).alias(key))
                orderby_agg_cols.append(key)
                if c in orderby_copy_cols_dict:
                    orderby_recover_cols_dict[key]=f"{orderby_copy_cols_dict[c]}{sep}{f.__name__}" 
        # 处理非字符型orderby
        order_int_list = [k for k, v in sdf.dtypes if k in orderby and len(
            re.findall(re.compile(r'(int)'), v)) > 0]
        order_float_list = [k for k, v in sdf.dtypes if k in orderby and len(
            re.findall(re.compile(r'(float|double)'), v)) > 0]
        if order_int_list:
            logstr('order_int_list', order_int_list)
            order_int_max_sdf = sdf.select(order_int_list).agg(
                *[F.max(c).alias(c) for c in order_int_list])
            order_int_max_df = sdf.select(order_int_list).agg(
                *[F.max(c).alias(c) for c in order_int_list]).toPandas()
            order_int_max_dict = dict(
                zip(order_int_max_df.keys(), order_int_max_df.values.flatten().tolist()))
            logstr('order_int_max_dict', order_int_max_dict)
            for c in order_int_list:
                sdf = sdf.withColumn(c, F.lpad(F.col(c).cast(
                    StringType()), len(str(order_int_max_dict[c])), '0'))
        if order_float_list:
            logstr('order_float_list', order_float_list)
            for c in order_float_list:
                sdf = sdf.withColumn(c, F.col(c).cast(StringType()))
                max_df = sdf.select(F.split(c, r"\.").alias(c)).select([F.length(F.col(c)[i]).alias(
                    c+f"__{i}") for i in range(2)]).agg(*[F.max(c+f"__{i}").alias(c+f"__{i}") for i in range(2)]).toPandas()
                max_dict = dict(
                    zip(max_df.keys(), max_df.values.flatten().tolist()))
                logstr('max_dict', max_dict)
                sdf = sdf.withColumn(c, F.lpad(F.col(c).cast(StringType()), max_dict[c+"__0"], '0')).withColumn(
                    c, F.rpad(F.col(c).cast(StringType()), max_dict[c+"__1"], '0'))
        agg_fun_list = [F.sort_array(F.collect_list(
            f"%s{sep}{c}" % '_'.join(orderby)),asc=ascending).alias(c+'_temp') for c in cols]
        # 这里对于Null值的处理仍不友好，即空值会以['a',,'b']这种形式给出
        sdf = sdf.select([F.concat_ws(
            sep, *orderby, c).alias(f"%s{sep}{c}" % '_'.join(orderby)) for c in cols]+groupby+orderby)
        sdf = sdf.groupBy(groupby).agg(*(agg_fun_list+orderby_agg_func))
        sdf = sdf.select([split_udf(c+'_temp').alias(c+suffix)
                      for c in cols]+orderby_agg_cols+groupby)
    else:
        agg_fun_list = [F.collect_list(c).alias(c+'_temp') for c in cols]
        sdf = sdf.select(cols+groupby+orderby)
        sdf = sdf.groupBy(groupby).agg(*(agg_fun_list+orderby_agg_func))
        sdf = sdf.select([F.col(c+'_temp').cast(ArrayType(dtype)).alias(c+suffix)
                          for c in cols]+orderby_agg_cols+groupby)
    for c1,c2 in orderby_recover_cols_dict.items():
        sdf=sdf.withColumnRenamed(c1,c2)
    return sdf

def sdf_pooling_sequence(sdf,col=None,length=None,mode='mean'):
    if col is None:
        col=sdf.columns[0]
    if length is None:
        length=sdf.select(F.size(col).alias('length')).take(1)[0]['length']
    sdf=sdf.select([F.col(col)[i].alias(f'temp_{i}') for i in range(length)])
    sdf=eval(f"sdf.groupby().{mode}()")
    sdf=sdf.select(F.array(sdf.columns).alias(col))
    return sdf
#     value=sdf.select(F.array(sdf.columns).alias(col)).take(1)[0][col]
#     return value

def sdf_join_by_dataskew(raw_sdf,join_sdf,raw_column,join_column=None,how='inner',skew_factor=10):#,join_drop=True,suffix='__y'
    """
    原始join：raw_sdf.join(join_sdf,on=raw_sdf.raw_column==join_sdf.join_column,how=how)
    现在join：sdf=join_by_data_skew(temp2,join_sdf,raw_column,join_column,how)
    """
    logstr('sdf_join_by_dataskew')
    if join_column is None:
        join_column = raw_column
    if not isinstance(raw_column,list):
        raw_column=[raw_column]
    if not isinstance(join_column,list):
        join_column=[join_column]
    assert skew_factor>0 and len(join_column)==len(raw_column)
    sep='___'
    random_list=range(skew_factor)
    join_column_randrank=f"{'_'.join(raw_column)}{sep}randrank"
    raw_sdf=raw_sdf.withColumn(join_column_randrank,F.concat_ws(sep,*raw_column,get_addrand_Column(random_list)))
    join_sdf=join_sdf.withColumn(f'{sep}randrank',get_addlist_Column(random_list))
    join_sdf=join_sdf.select(*move_list(join_sdf.columns,f'{sep}randrank'),F.explode(f'{sep}randrank').alias(f'{sep}randrank'))
    join_sdf=join_sdf.withColumn(join_column_randrank,F.concat_ws(sep,*join_column,f'{sep}randrank')).drop(f'{sep}randrank').drop(*join_column)
#     if join_drop:
#         join_sdf=join_sdf.drop(*join_column)
#     else:
#         if len(set(join_column) & set(raw_column))>0:
#             for x in join_column:
#                 join_sdf=join_sdf.withColumnRenamed(x,x+suffix)
    all_sdf=raw_sdf.join(join_sdf, on=join_column_randrank, how=how).drop(join_column_randrank) 
    return all_sdf

def sdf_join_by_dataskew_quantile(raw_sdf,join_sdf,raw_column,join_column=None,how='inner',skew_factor=10,skew_quantile=0.90):
    logstr('sdf_join_by_dataskew_quantile newest')
    if join_column is None:
        join_column = raw_column
    if not isinstance(raw_column,list):
        raw_column=[raw_column]
    if not isinstance(join_column,list):
        join_column=[join_column]
    assert skew_factor>0 and len(join_column)==len(raw_column) and skew_quantile>0 and skew_quantile<=1
    for i,c in enumerate(join_column):
        if c!=raw_column[i]:
            join_sdf=join_sdf.withColumnRenamed(c,raw_column[i])
    skew_flag='___skew_flag___'
    raw_sdf_count=raw_sdf.groupby(raw_column).count()
#     raw_sdf_count.cache()
#     raw_sdf_count.count()
    skew_quantile_len=sdf_percentile(raw_sdf_count,percentile=[skew_quantile],cols='count')['count'][skew_quantile]
    skew_temp_raw_sdf=raw_sdf_count.where(f"count>={skew_quantile_len}").select(raw_column)
    skew_temp_raw_sdf.cache()
#     logstr(f'skew lens of {raw_column} Columns',skew_temp_raw_sdf.count())
    skew_join_sdf=skew_temp_raw_sdf.join(join_sdf,on=raw_column,how='inner')
    raw_sdf2=raw_sdf.join(skew_temp_raw_sdf.withColumn(skew_flag,F.lit(1)),on=raw_column,how='left')
    skew_raw_sdf=raw_sdf2.where(f"{skew_flag} is not null").drop(skew_flag)
    not_skew_raw_sdf=raw_sdf2.where(f"{skew_flag} is null").drop(skew_flag)
    skew_temp_raw_sdf.unpersist()

    all_sdf1=not_skew_raw_sdf.join(join_sdf,on=raw_column,how=how)
    all_sdf2=sdf_join_by_dataskew(skew_raw_sdf,skew_join_sdf,raw_column,join_column,how,skew_factor)
    all_sdf=all_sdf1.select(all_sdf2.columns).union(all_sdf2)
    return all_sdf

# pyspark dataframe 序列化特征内部join
def mapping_sequence(sdf, join_sdf, column='item_id_collect__sub', join_column=None, how='inner', dtype=ArrayType(ArrayType(FloatType())),fillna=None,drop=False,skew_quantile=0.9,orderby_flag=True):
    """
    Paramter:
    ----------
    sdf: pyspark dataframe to be processed
    join_sdf: pyspark dataframe which be joined by sdf 
    column: str of sdf's col to be mapping
    join_column: str of join_sdf's col to be joined by sdf
    how: str of the join mode which sdf.join(join_sdf)
    dtype: pyspark.sql.types of the return values
    Return:
    ----------
    sdf: pyspark dataframe of sdf.join(join_sdf) by mapping_sequence
    Example:
    ----------
    sdf=mapping_sequence(sdf,join_sdf,column='item_id_collect__sub')
    """
    if join_column is None:
        join_column = column
    resdtype = len(re.findall(re.compile(r'(ArrayType)'), str(dtype)))
    if resdtype == 1:
        def array_eval_udf(x): return F.col(x).cast(dtype)
    else:
        array_eval_udf = F.udf(lambda x: [eval(i)
                                          for i in x], dtype)
    join_other_list = [
        c+'__explode' for c in join_sdf.columns if c != join_column]
    def other_list_fun(all_, pop_): return list(
        filter(lambda x: x not in pop_, all_))
    temp1 = sdf.withColumn('increasing_id', F.monotonically_increasing_id())
    temp2 = temp1.select(F.posexplode(column).alias(
        column+'__explodeindex', column+'__explodecol'), *temp1.columns)
    join_sdf = eval("join_sdf.withColumnRenamed"+".withColumnRenamed".join([f"('{c}','%s')" % (
        f"{c}__explode" if c != join_column else column+'__explodecol') for c in join_sdf.columns]))
    if skew_quantile is not None:
#         temp2=sdf_join_by_dataskew(temp2,join_sdf,raw_column=column+'__explodecol',how=how)
        temp2=sdf_join_by_dataskew_quantile(temp2,join_sdf,raw_column=column+'__explodecol',how=how,\
                                            skew_quantile=skew_quantile)
    else:
        temp2 = temp2.join(join_sdf, on=column+'__explodecol', how=how)
    if fillna is not None:
        temp2=temp2.fillna(dict(zip(join_other_list,[fillna]*len(join_other_list))))    
    temp2 = temp2.select(other_list_fun(temp2.columns, join_other_list) +
                         [cast2str_udf(c).alias(c) for c in join_other_list])
#     temp2.cache()
    collect_temp2 = collect_orderby(
        temp2, join_other_list, groupby=temp1.columns, orderby=column+'__explodeindex' if orderby_flag else None)
#     return collect_temp2,join_other_list
#     temp2.unpersist()
    res_other = [array_eval_udf(c+'__collect').alias(c+'__collect')
                 for c in join_other_list]
    sdf = collect_temp2.select(other_list_fun(collect_temp2.columns, [
                               c+'__collect' for c in join_other_list])+res_other).drop('increasing_id')
#     sdf = sdf.select([c+'__collect' for c in join_other_list]+sdf.columns).drop('increasing_id')
    for c in join_other_list:
        sdf=sdf.withColumnRenamed(c+'__collect',c.split('__explode')[0]+'__collect')
    if drop:
        sdf=sdf.drop(column)
    return sdf


