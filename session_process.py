
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
from ..base import logger,logstr


__all__=['get_merge_by_time_udf']

############## session处理类
# 计算2个时间的时间差

def times_diff(*k,type='mins',format="%Y-%m-%d %H:%M:%S"):
    import datetime
    assert type in ["days","seconds"]+["mins","minutes","hours"]
    div_dict={"mins":60,"minutes":60,"hours":60*60,'seconds':1}
    if isinstance(k[0],list):
        k=k[0]
    assert len(k)==2
    k=[datetime.datetime.strptime(i, format) for i in k]
    if type in ["days"]:
        res=eval(f"(k[0] - k[1]).{type}")
    else:
        res=eval(f"(k[0] - k[1]).total_seconds()")
        if type in div_dict:
            res/=div_dict[type]
    return res
 
# 计算与零点的绝对时间差
def times_diff_for_bounds(x,mode="total",type="mins",format="%Y-%m-%d %H:%M:%S"):
    import datetime
    assert mode in ["first","last","total"]
    now_time=datetime.datetime.strptime(x, format)
    compare_time=[]
    if mode in ["first","total"]:
        compare_time.append(now_time.date().strftime(format))
    if mode in ["last","total"]:
        compare_time.append((now_time.date()+datetime.timedelta(days=1)).strftime(format))
    res=[abs(times_diff(x,k,type=type,format=format)) for k in compare_time]
    return res[0] if len(res)==1 else min(res)
 
# 单item、单时间合并(no sessionid)
def get_merge_by_time_udf(min_time_sep=10,max_time_sep=None,type="mins",keep_time="max",format="%Y-%m-%d %H:%M:%S",get_dt_merge_flag=False):
    @F.udf(ArrayType(StringType()))
    def merge_by_time_udf(col):
        """
        ['[time1,item1]','[time2,item2]',...]
        """
        def dt_merge_flag_f(i,j,time):# 是否需要跨天合并 -1,0,1
            res=0
            if i==0 and times_diff_for_bounds(time,mode="first",type=type,format=format)<=min_time_sep:
                res=-1
            if (j!=i or res==0) and j==len(col)-1 and times_diff_for_bounds(time,mode="last",type=type,format=format)<=min_time_sep:
                res=1
            return res
        get_col_value_f=lambda i:eval(col[i]) if isinstance(col[i],str) else col[i]
        i=j=0
        all_res=[]
        if len(col)==1: #长度为1时
            time,item=get_col_value_f(i)
            if get_dt_merge_flag:
                dt_merge_flag=dt_merge_flag_f(i,j,time)
                return [str([time,str([item]),dt_merge_flag])]
            else:
                return [str([time,str([item])])]
        while i<len(col)-1:
    #             logstr("i",i)
            time,item=get_col_value_f(i)
            now_time=time
            item_list=[item]
            for j in range(i+1,len(col)):
    #                 logstr("j",j)
                if get_dt_merge_flag:
                    dt_merge_flag=dt_merge_flag_f(i,j,time)
                time_next,item_next=get_col_value_f(j)
                flag=(max_time_sep is None or times_diff(time_next,time,type=type,format=format)<=max_time_sep) and times_diff(time_next,now_time,type=type,format=format)<=min_time_sep
                if flag:# 判断是否被划分为下一个session
                    now_time=time_next
                    item_list.append(item_next)
    #                 session_id=f"{session_id}__{session_id_next}"
                if not flag or j==len(col)-1: # 划分下一个session或为最后一个元素时
                    all_res.append(str([now_time if keep_time=="max" else time,str(item_list)]+([dt_merge_flag] if get_dt_merge_flag else [])))
                    if not flag and j==len(col)-1:
                        if get_dt_merge_flag:
                            dt_merge_flag=dt_merge_flag_f(i,j,time_next)
                            all_res.append(str([time_next,str([item_next]),dt_merge_flag]))
                        else:
                            all_res.append(str([time_next,str([item_next])]))
    #                     logstr("all_res",all_res)
                    i=j
                    break
        return all_res
    return merge_by_time_udf
 
# # 单item、单时间合并
# def get_merge_by_1time_udf(min_time_sep=10,max_time_sep=None,type="mins",keep_time="max",format="%Y-%m-%d %H:%M:%S",get_dt_merge_flag=True):
#     @F.udf(ArrayType(StringType()))
#     def merge_by_1time_udf(col):
#         """
#         ['[session_id1,time1,item1]','[session_id2,time2,item2]',...]
#         """
#         i=j=0
#         all_res=[]
#         def dt_merge_flag_f(i,j,time):# 是否需要跨天合并 -1,0,1
#             res=0
#             if i==0 and times_diff_for_bounds(time,mode="first",type=type)<=min_time_sep:
#                 res=-1
#             if (j!=i or res==0) and j==len(col)-1 and times_diff_for_bounds(time,mode="last",type=type)<=min_time_sep:
#                 res=1
#             return res
#         def get_col_value_f(i):
#             return eval(col[i]) if isinstance(col[i],str) else col[i]
 
#         if len(col)==1:
#             if get_dt_merge_flag:
#                 session_id,time,item=get_col_value_f(i)
#                 dt_merge_flag=dt_merge_flag_f(i,j,time)
#                 return [str([*eval(col[j]),dt_merge_flag])]
#             else:
#                 return [col[j]]
#         while i<len(col)-1:
#     #             logstr("i",i)
#             session_id,time,item=get_col_value_f(i)
#             now_time=time
#             item_list=[item]
#             for j in range(i+1,len(col)):
#     #                 logstr("j",j)
#                 if get_dt_merge_flag:
#                     dt_merge_flag=dt_merge_flag_f(i,j,time)
#                 session_id_next,time_next,item_next=get_col_value_f(j)
#                 flag=(max_time_sep is None or times_diff(time_next,time,type=type)<=max_time_sep) and times_diff(time_next,now_time,type=type)<=min_time_sep
#                 if flag:
#                     now_time=time_next
#                     item_list.append(item_next)
#                     session_id=f"{session_id}__{session_id_next}"
#                 if not flag or j==len(col)-1:
#                     all_res.append(str([session_id,now_time if keep_time=="max" else time,item_list]+([dt_merge_flag] if get_dt_merge_flag else [])))
#                     if not flag and j==len(col)-1:
#                         if get_dt_merge_flag:
#                             dt_merge_flag=dt_merge_flag_f(i,j,time_next)
#                             all_res.append(str([*eval(col[j]),dt_merge_flag]))
#                         else:
#                             all_res.append([col[j]])
#     #                     logstr("all_res",all_res)
#                     i=j
#                     break
#         return all_res
#     return merge_by_1time_udf
 
# # item_list、双时间合并
# def get_merge_by_2time_udf(min_time_sep=10,max_time_sep=None,type="mins",format="%Y-%m-%d %H:%M:%S",get_dt_merge_flag=True):
#     @F.udf(ArrayType(StringType()))
#     def merge_by_2time_udf(col):
#         """
#         ['[session_id1,first_time1,last_time1,"item_list1"]','[session_id2,first_time2,last_time2,"item_list2"]',...]
#         """
#         i=j=0
#         all_res=[]
#         def dt_merge_flag_f(i,j,first_time,last_time):# 是否需要跨天合并 -1,0,1
#             res=0
#             if i==0 and times_diff_for_bounds(first_time,mode="first",type=type,format=format)<=min_time_sep:
#                 res=-1
#             if (j!=i or res==0) and j==len(col)-1 and times_diff_for_bounds(last_time,mode="last",type=type,format=format)<=min_time_sep:
#                 res=1
#             return res
 
#         def get_col_value_f(i):
#             session_id,first_time,last_time,item_list=eval(col[i]) if isinstance(col[i],str) else col[i]
#             if isinstance(item_list,str):
#                 item_list=eval(item_list)
#             return session_id,first_time,last_time,item_list
 
#         if len(col)==1:
#             if get_dt_merge_flag:
#                 session_id,first_time,last_time,item_list=get_col_value_f(i)
#                 dt_merge_flag=dt_merge_flag_f(i,j,first_time,last_time)
#                 return [str([*eval(col[j]),dt_merge_flag])]
#             else:
#                 return [col[i]]
#         while i<len(col)-1:
# #             logstr("i",i)
#             session_id,first_time,last_time,item_list=get_col_value_f(i)
#             now_time=last_time
#             for j in range(i+1,len(col)):
# #                 logstr("j",j)
#                 if get_dt_merge_flag:
#                     dt_merge_flag=dt_merge_flag_f(i,j,first_time,now_time)
#                 session_id_next,first_time_next,last_time_next,item_list_next=get_col_value_f(j)
#                 flag=(max_time_sep is None or times_diff(last_time_next,first_time,type=type,format=format)<=max_time_sep) and times_diff(first_time_next,now_time,type=type,format=format)<=min_time_sep
#                 if flag:
#                     now_time=last_time_next
#                     item_list.extend(item_list_next)
#                     session_id=f"{session_id}__{session_id_next}"
#                 if not flag or j==len(col)-1:
#                     all_res.append(str([session_id,first_time,now_time,item_list]+([dt_merge_flag] if get_dt_merge_flag else [])))
#                     if not flag and j==len(col)-1:
#                         if get_dt_merge_flag:
#                             dt_merge_flag=dt_merge_flag_f(i,j,first_time_next,last_time_next)
#                             all_res.append(str([*eval(col[j]),dt_merge_flag]))
#                         else:
#                             all_res.append([col[j]])
# #                     logstr("all_res",all_res)
#                     i=j
#                     break
#     #     logstr("all_res",all_res)
#         return all_res
#     return merge_by_2time_udf
