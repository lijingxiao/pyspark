## array均值
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
