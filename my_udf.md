1)普通函数注册为pyspark udf
```python
@F.udf(ArrayType(ArrayType(StringType()))
def f_udf(x):
       return 

f_udf=F.udf(lambda x:f(x),ArrayType(ArrayType(StringType())))
```

2)pyspark udf传参类型为pyspark.sql.column.Column，即sdf.a或F.col('a')
```python
def get_standard_udf(column):
    mean=mean_dict[column]
    stddev=stddev_dict[column]
    def standard(column,mean,stddev):
        if stddev!=0 and (isinstance(column,float) or isinstance(column,int)): #这里的判断条件在一定前提下可不要
            res=(column-mean)/stddev
        else:
            res=0.0
        return float(res) #必要时请一定要注意类型转换！spark2.2.0后的sparkSql不能使用数据类型为numpy内置的dtype的数据！
    standard_udf=udf(lambda x:standard(x,mean,stddev),DoubleType())
    return standard_udf
```

3)输出类型：不能使用数据类型为numpy内置的dtype
```python
type(np.random.choice(np.arange(10))) #numpy.int64
```

## array均值
其实通过拆解成多列，求avg然后再合并的方式更快
```python
import pyspark.sql.functions as F
from pyspark.sql.types import  *
# 以下三种方式都可以实现,np.mean(可以直接传hive的array)
# 一般ndarray转list都是用tolist()方法，该方法会递归转list，不要使用list(array)这种方式
def get_pooling_sequence_udf(mode='mean', axis=0, dtype=FloatType()):
    def pooling(x, mode, axis):
        import numpy as np
        return (eval(f"np.array(x).{mode}(axis={axis})")).tolist()# 注意不要直接list(np.array)，要tolist
    pooling_udf=F.udf(lambda x: pooling(x, mode, axis), ArrayType(dtype))
    return pooling_udf
def array_mean(array):
    import numpy as np
    array_mean_array = np.array(array).mean(axis=0)
    array_mean_lst = array_mean_array.tolist()
    return array_mean_lst
array_mean_udf = F.udf(array_mean,ArrayType(FloatType()))

def array_mean2(array):
    import numpy as np
    array_mean_lst = np.mean(array,axis=0).tolist()
    return array_mean_lst
array_mean2_udf = F.udf(array_mean2,ArrayType(FloatType()))
```

## array拆解为多列
```python
def get_split_array_udf(*name,prefix=None,length=None):
    """
    #返回多列，sdf.select(get_split_array_udf("a","b","c")("array_col"))
    #不可直接sdf.withColumn
    """
    assert name or prefix and length
    if name:
        if isinstance(name[0],list):
            name=name[0]
    else:
        name=[f'{prefix}_{i}' for i in range(1,length+1)]
    def split_array_udf(array_col):
        return [F.col(array_col)[i].alias(name[i]) for i in range(len(name))]
    return split_array_udf
```

## Vector转Array
```python
vector2array_udf = F.udf(lambda x: x.toArray().tolist(), ArrayType(DoubleType()))
```

## Array(或单变量)转Vector[必有数值]
```python
array2vector_udf = F.udf(lambda x: Vectors.dense(x), VectorUDT())
```

## any转str
```python
cast2str_udf=F.udf(lambda x:str(x),StringType())
```

## DenseVector转SparseVector
```python
dense2sparse_udf = F.udf(lambda vector: _convert_to_vector(
    scipy.sparse.csc_matrix(vector.toArray()).T), VectorUDT())
```

## SparseVector转DenseVector[必有数值]
```python
sparse2dense_udf = F.udf(lambda x: Vectors.dense(x), VectorUDT())
```

```python
def get_concat_udf(sep='___',dtype=ArrayType(StringType())):
    @F.udf(dtype)
    def concat_udf(*x):
        return [sep.join(i) for i in zip(*x)]
    return concat_udf

def get_fillna_udf(default,dtype=StringType()):
    @F.udf(dtype)
    def fillna_udf(x):
        return x if x else default
    return fillna_udf(x)
    
def get_eval_udf(mode="str",dtype=ArrayType(StringType())):
    """"
    mode:{'str':eval(StringType()),'array':[eval(StringType()) for StringType() in ArrayType(StringType())]}
    """
    @F.udf(dtype)
    def eval_udf(x): 
        return eval(x) if mode in ['str','string'] else [eval(i) for i in x]
    return eval_udf
    
def get_addlist_Column(x,dtype=None):
    """
    多维时必须各维度数量一致
    """
    import numpy as np
    if dtype is None and len(np.array(x).shape)==1:
        return F.array([F.lit(i) for i in x]) #fast
    else:
        assert dtype is not None
        return get_eval_udf(dtype=dtype)(F.lit(str(x)))
    
def get_addrand_Column(x,dtype=IntegerType()):
    import random
    @F.udf(dtype)
    def rand_udf(col):
        return random.choice(x)
    return rand_udf(F.lit(0))
    
def get_row_number_Column(partitionBy=None,orderBy=None,ascending=True,start_index=1):
    if partitionBy is None:
        partitionBy=[]
    if orderBy is None:
        orderBy=[F.rand(123)]#orderBy不能为空
    if not isinstance(partitionBy,list):
        partitionBy=[partitionBy]
    if not isinstance(orderBy,list):
        orderBy=[orderBy]
    if not ascending and orderBy is not None:#实际上随机时仅能这么写F.rand().desc()
        orderBy=[F.desc(c) for c in orderBy] # F.col(c).desc()
    return F.row_number().over(Window.partitionBy(*partitionBy).orderBy(*orderBy))+start_index-1
```







