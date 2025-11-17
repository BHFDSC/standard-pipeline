# Databricks notebook source
# count_var
# count_varlist
# tab
# tabstat
# rename_columns
# merge
# reshape_wide_to_long
# reshape_wide_to_long_multi
# reshape_long_to_wide
# check_id
# check_null
# null_safe_equality
# compare_files
# sdc
# codelist_match
# codelist_match_summ
# create_table
# drop_table
# temp_save
# save_table
# table_summ
# get_provisioning_date
# union_dataframe_list

# zw


# COMMAND ----------

# from pyspark.sql import DataFrame as SparkDataFrame
import warnings

# COMMAND ----------

def count_var(df, var1: str, ret=0, df_desc='', indx=0) -> None:
  '''
  Counts the number of rows, number of non-missing rows, number of 
  non-missing and distinct rows for a variable 'var1' in a Spark or 
  Pandas DataFrame. 
  
  The function prints the counts and if ret = 1 
  it will return the counts as a SparkDataFrame.
  
  If the DataFrame is a pd.DataFrame only objects will be counted.
  
  If the DataFrame is a Spark DataFrame and var1 is "LongType", 
  'DateType','DecimalType(19,0)'' and 'StringType' the counts 
  will be generated.
  
  Args:
    df: Spark, Koalas or Pandas DataFrame.
    var1: String of a column in a DataFrame Schema.
    ret: Flag indicating the return if a SparkDataFrame, default=0.
    df_desc: A description of the the DataFrame, only used 
             if ret = 1.     
    indx: Only used if ret=1, default=0.
    
  Example usage: Doc Test format? 
    count_var(df,'DIAG_03')
    >> 
    
  Notes:
    - Importing libraries inside a function, it may be better if these 
      were brought outside the function to avoid repetiton.
    
    - Any Koalas DataFrames will be converted to Spark DataFrames.
    
    - Not quite sure what flush = True does. I think it is trying to
      free memory, however can't find this parameter in a pd.DataFrame
      or a Spark DataFrame.
    
    - Could make use of try/except to handle errors
      try:
        Block of code
      except: 
        When logic fails handle the error this way.
        
  '''
  
  import pyspark.sql.functions as f
  import databricks.koalas as ks
  import pandas as pd
  
  #Not sure why this list comprehension is used instead of 
  #passing the variable 'df'. 
  dfname = [x for x in globals() if globals()[x] is df]
  
  if(len(dfname) == 1): 
    dfname = dfname[0]
  else: 
    dfname = "unknown dfname (not in globals())"
  
  # If a koalas dataframe is passed to the function, it will be converted
  # to a spark DataFrame. Is importing koalas is redundant?
  if isinstance(df, ks.DataFrame):
    df = df.to_spark()
    
  if isinstance(df, pd.DataFrame): 
    dftype = 'pandas'
    var1type = df[var1].dtype.name
    
    #pd.DataFrame does not have a LongType data type.
    #so this if statement will never run?
    if(str(var1type) == "LongType"):
      #More meaningful naming conventions someting more like
      #index_count, non_null_index_count, non_null_unique_count   
      count1 = len(df.index)
      count2 = len(df[var1][df[var1].notnull()].index)
      count3 = len(df[var1][df[var1].notnull()].unique())
    
    # This loop will always execute first if the 
    # instance of df is a pd.DataFrame   
    elif(str(var1type) == "object"):
      #This approach would mean repeating count1, count2, count3
      #for every datatype in a pd.DataFrame and looks the same as
      #LongType, which pd.DataFrames dont have.       
      count1 = len(df.index)
      count2 = len(df[var1][df[var1].notnull()].index)
      count3 = len(df[var1][df[var1].notnull()].unique())
    else:
      #Maybe put the the print statement within the raising of error type.
      #prints var1type as a string, could follow with printing the real
      #type of var1type with type(var1type)
      #I think flush tries to free memory, but don't think a pd.DataFrame
      #has this parameter?    
      print("  Not coded for this dataType! " + str(var1type), flush=True)
      #This should probably be a TypeError.   
      raise ValueError('...')
  else: 
    dftype = 'spark'
    var1type = df.schema[var1].dataType  
    if(str(var1type) in("LongType", 'DateType', 'DecimalType(19,0)', 'LongType()', 'DateType()')):
      count1 = df.count()
      count2 = df.select(var1).filter(f.col(var1).isNotNull()).count()
      count3 = df.select(var1).filter(f.col(var1).isNotNull()).distinct().count()
    elif(str(var1type) in ("StringType", "StringType()")):
      count1 = df.count()
      count2 = df.select(var1).filter((f.col(var1).isNotNull()) & (f.trim(f.col(var1)) != "")).count() 
      count3 = df.select(var1).filter((f.col(var1).isNotNull()) & (f.trim(f.col(var1)) != "")).distinct().count() 
    else:
      #Maybe put the the print statement within the raising of Exception.  
      print("  Not coded for this dataType! " + str(var1type), flush=True)   
      raise Exception
    
  print(f'counts ({dfname})', flush=True)
  # print("  df=" + dfname + " (" + dftype + "), var1=" + var1 + " (" + str(var1type) + ").", flush=True)
  # print("  {0:<45}".format('N(rows)') + "=" +  f'{count1:15,d}', flush=True)
  # print("  {0:<45}".format(f'N(non-missing {var1})') + "=" + f'{count2:15,d}', flush=True)
  # print("  {0:<45}".format(f'N(non-missing and distinct {var1})') + "=" + f'{count3:15,d}', flush=True)   
  
  tm1 = f'N(rows)'
  tm2 = f'N(non-missing {var1})'
  tm3 = f'N(non-missing and distinct {var1})'
  tmc = len(' ' + tm3) 
  
  countm = max(count1, count2, count3)
  tmm = len(' ' + f'{countm:,}')
 
  print("  " + f"{tm1:<{tmc}}" + "= " + f'{count1:{tmm},d}', flush=True)
  print("  " + f"{tm2:<{tmc}}" + "= " + f'{count2:{tmm},d}', flush=True)
  print("  " + f"{tm3:<{tmc}}" + "= " + f'{count3:{tmm},d}', flush=True)
  
  #There is only an if statement with no else clause 
  if(ret == 1):
    tmp = spark.createDataFrame(
      [
        (f'{indx}', f'{dfname}', f'{df_desc}', f'{var1}', f'{count1}', f'{count2}', f'{count3}'),
      ],
      ['indx', 'df_name', 'df_desc', 'var', 'n', 'n_id', 'n_id_distinct']  
    )
    return tmp

# COMMAND ----------

def count_varlist(df, varlist): 
  
  import pyspark.sql.functions as f
  import databricks.koalas as ks
  import pandas as pd
  
  dfname = [x for x in globals() if globals()[x] is df]
  if(len(dfname) == 1): dfname = dfname[0]
  else: dfname = "unknown dfname (not in globals())"
    
  dt = dict(df.dtypes)
  count1 = df.count()
  tmp = df\
    .select(varlist)
  for var in varlist:
    var1type = dt[var]
    if(str(var1type) in("LongType", 'DateType', 'DecimalType(19,0)', 'date', 'bigint', 'int', 'decimal(14,4)')):
      tmp = tmp\
        .where(f.col(var).isNotNull())
    elif(str(var1type) == "string"):      
      tmp = tmp\
        .where((f.col(var).isNotNull()) & (f.trim(f.col(var)) != ""))      
    else:
      print("  Not coded for this dataType! " + str(var1type), flush=True)
      raise Exception      
  count2 = tmp.count()  
  count3 = tmp.distinct().count()
 
  print(f'counts ({dfname})', flush=True)
  tm1 = f'N(rows)'
  tm2 = f'N(non-missing {varlist})'
  tm3 = f'N(non-missing and distinct {varlist})'
  tmc = len(' ' + tm3) 
  countm = max(count1, count2, count3)
  tmm = len(' ' + f'{countm:,}')
  print("  " + f"{tm1:<{tmc}}" + "= " + f'{count1:{tmm},d}', flush=True)
  print("  " + f"{tm2:<{tmc}}" + "= " + f'{count2:{tmm},d}', flush=True)
  print("  " + f"{tm3:<{tmc}}" + "= " + f'{count3:{tmm},d}', flush=True)
  print()


# COMMAND ----------

def tab(df, var1, var2=None, var2_wide=1, var2_unstyled=1, cumsum=0):
  import pyspark.sql.functions as f 
  import databricks.koalas as ks
  import pandas as pd
  import numpy as np
  
  # dfname = [x for x in globals() if globals()[x] is df]
  # if(len(dfname) == 1): dfname = dfname[0]
  # else: dfname = "unknown (not in globals())"
  
  if isinstance(df, pd.DataFrame): 
    dftype = 'pandas'
    var1type = df[var1].dtype.name
  elif isinstance(df, ks.DataFrame):
    df = df.to_spark()
    dftype = 'spark'
    var1type = df.schema[var1].dataType   
  else: 
    dftype = 'spark'
    var1type = df.schema[var1].dataType
  
  # print('\ntab', flush=True)
  if(var2 is None):    
    # print("  df=" + dfname + " (" + dftype + "), var1=" + var1 + " (" + str(var1type) + ").", flush=True)

    if(dftype == "pandas"):
      if(str(var1type) in ["category"]): df[var1] = df[var1].astype(str)
        
      #tmp = df.groupby([var1], dropna=False).size().reset_index(name='N').sort_values([var1])
      tmp = df[var1].value_counts(dropna=False).reset_index(name='N').rename(columns={'index': var1})
      tmp['tmp'] = np.where(tmp[var1].isnull(), 1, 2)
      tmp = tmp.sort_values(['tmp', var1], ignore_index=True).drop(columns=['tmp'])
    else: 
      tmp = df\
        .groupBy(var1)\
        .count()\
        .withColumnRenamed('count', 'N')\
        .orderBy(var1)        
      if(cumsum == 1):
        _win_cumsum = Window\
          .partitionBy()\
          .orderBy(f.col(var1))\
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        tmp = tmp\
          .withColumn('N_CUMSUM', f.sum('N').over(_win_cumsum))
      tmp = tmp\
        .toPandas()
      
    if(str(var1type) in ["IntegerType", "int64", "int32"]): tmp[var1] = tmp[var1].map('{:,.0f}'.format)
      
    tmp['PCT'] = (100 * tmp['N'] / tmp['N'].sum()) #.round(1)
    if(cumsum == 1): tmp['PCT_CUMSUM'] = (100 * tmp['N_CUMSUM'] / tmp['N'].sum()) #.round(1)    
    tmp.loc['Total', 'N'] = tmp['N'].sum()
    tmp.loc['Total', 'PCT'] = tmp['PCT'].sum()
    tmp.loc['Total', var1] = ''
    if(cumsum == 1): 
      tmp.loc['Total', 'N_CUMSUM'] = tmp['N_CUMSUM'].iloc[-2]
      tmp.loc['Total', 'PCT_CUMSUM'] = tmp['PCT_CUMSUM'].iloc[-2]
      # print(tmp)
      tmp = tmp.astype({"N_CUMSUM": int})
      tmp["N_CUMSUM"] = tmp["N_CUMSUM"].map('{:,d}'.format)
      tmp['PCT_CUMSUM'] = tmp['PCT_CUMSUM'].round(1)
   
    tmp = tmp.astype({"N": int})
    tmp["N"] = tmp["N"].map('{:,d}'.format)
    tmp['PCT'] = tmp['PCT'].round(1)
      
      
    pd.set_option('display.max_rows', len(tmp))
    print(tmp, flush=True)
    pd.reset_option('display.max_rows')
    
  else:  
    if(dftype == "pandas"): var2type = df[var2].dtype.name
    else: var2type = df.schema[var2].dataType
    # print("df=" + dfname + " (" + dftype + "), var1=" + var1 + " (" + str(var1type) + "), var2=" + var2 + " (" + str(var2type) + ").", flush=True)
    
    # convert categorical to string to ensure that NaN's are shown...
    if(str(var1type) in ["category"]): df[var1] = df[var1].astype(str)
    if(str(var2type) in ["category"]): df[var2] = df[var2].astype(str)    
    
    if(dftype == "pandas"): tmp = df.groupby([var1, var2], dropna=False).size().reset_index(name='N').sort_values([var1, var2])
    else: tmp = df.groupBy(var1,var2).count().withColumnRenamed('count', 'N').orderBy(var1,var2).toPandas()
      
    if(var2_wide == 1):      
      tmp = tmp.astype({"N": int})      
            
      # pivot
      #tmp = tmp.pivot(var2).agg(first('N'))
      tmp = tmp.pivot(index=var1, columns=var2, values='N').fillna(0)            
      
      # Total sum per column: 
      #if(str(var1type) in ["category"]): tmp.index = tmp.index.add_categories('Total')
      tmp.loc['Total',:]= tmp.sum(axis=0)
      #tmp.loc['Total'] = tmp.sum(axis=0)   
      
      #Total sum per row: 
      ##if(str(var2type) in ["category"]): tmp.columns = tmp.columns.add_categories('Total')
      tmp.loc[:,'Total'] = tmp.sum(axis=1)
      
      # format
      cols = tmp.columns.values
      for col in cols:
        tmp[col] = tmp[col].astype('int') 
        tmp[col] = tmp[col].map('{:,d}'.format)
    
      # headings
      tmp.index.name = None
      tmp = tmp.rename_axis(var1, axis=1)
      tmp.columns = pd.MultiIndex.from_product([[var2], tmp.columns])

      # style
      tmp_s = tmp.style\
        .set_properties(**{'width': '80px', 'text-align': 'right', 'font-size': '0.9em', \
                           'border-color': 'black','border-style' :'solid' ,'border-width': '0px','border-collapse': 'collapse'})\
        .set_table_styles([\
          dict(selector='td', props=[('border-collapse', 'collapse')]), \
          dict(selector='th', props=[('text-align', 'right'), ('background-color', '#edf2f9'), ('color', '#112277'), ('font-size', '0.9em'), ('border-collapse', 'collapse')]),\
          dict(selector='th.col_heading.level1', props=[('text-align', 'right'), ('background-color', '#edf2f9'), ('color', '#112277')]),\
          dict(selector='th.col_heading.level0', props=[('text-align', 'center'), ('background-color', '#edf2f9'), ('color', '#112277')])\
        ])
      
      # output
      if(var2_unstyled == 1):
        pd.set_option('display.max_rows', len(tmp))
        pd.set_option('display.max_columns', len(tmp.columns))
        pd.set_option('expand_frame_repr', False)        
        print(tmp, flush=True)
        pd.reset_option('display.max_rows')
        pd.reset_option('display.max_columns')
        pd.reset_option('expand_frame_repr')
      else:
        display(tmp_s)
        
    else: 
      tmp['PCT'] = (100 * tmp['N'] / tmp['N'].sum()) #.round(1)
      tmp.loc['Total'] = tmp.sum()
      tmp.loc['Total', var1] = ''
      tmp.loc['Total', var2] = ''
      tmp = tmp.astype({"N": int})
      tmp["N"] = tmp["N"].map('{:,d}'.format)
      tmp['PCT'] = tmp['PCT'].round(1)
      pd.set_option('display.max_rows', len(tmp))
      pd.set_option('display.max_columns', len(tmp.columns))
      pd.set_option('expand_frame_repr', False)   
      print(tmp, flush=True)
      pd.reset_option('display.max_rows')
      pd.reset_option('display.max_columns')
      pd.reset_option('expand_frame_repr')      
  
  return tmp

# COMMAND ----------

# work in progress... 
# def tabstat(df, var, byvar, date=0):
#   print (pd.DataFrame(df[var]).columns) 
#   tmp = pd.DataFrame(df[var]).apply(pd.to_datetime).describe(datetime_is_numeric=True)
#   tmp[var + '_str'] = tmp[var].astype(str) # #.astype(str)
#   tmp[var + '_str'] = tmp[var + '_str'].str.replace(r' \d+:\d+:\d+\.*\d+', '')
#   tmp = tmp.drop(columns=[var])
#   print(tmp)
#   return tmp

def tabstat(df, var, byvar=None, date=0):
  
  import pyspark.sql.functions as f
  import pandas as pd
  import re
  
  if(byvar is None):
    df = df\
      .withColumn('all1', f.lit(1))
    byvar = 'all1'
  
  var_int = var
  if(date==1): 
    var_int = f'{var}_int'
    df = df\
      .withColumn(f'{var_int}', f.datediff(f.col(var), f.to_date(f.lit('1960-01-01'))))     
    
  tmp = df\
    .groupBy(byvar)\
    .agg(\
      f.count(f.lit(1)).alias('n_rows')\
      , f.count(f.col(var)).alias('n')\
      , f.round(f.mean(f.col(f'{var_int}')),2).alias('mean')\
      , f.round(f.stddev(f.col(f'{var_int}')),2).alias('sd')\
      , f.min(f.col(var)).alias('min')\
      , f.max(f.col(var)).alias('max')\
      , f.expr(f'percentile({var_int}, array(0.05))')[0].alias('p05')\
      , f.expr(f'percentile({var_int}, array(0.25))')[0].alias('p25')\
      , f.expr(f'percentile({var_int}, array(0.50))')[0].alias('p50')\
      , f.expr(f'percentile({var_int}, array(0.75))')[0].alias('p75')\
      , f.expr(f'percentile({var_int}, array(0.95))')[0].alias('p95')\
    )\
    .orderBy(byvar)

    # f.count(f.lit(1)).alias('n')\
    # , f.countDistinct(f.col(var)).alias('n_var_disinct')\

  if(date==1):    
    for col in ['mean'] + [col for col in tmp.columns if re.match('^p[0-9][0-9]$', col)]:
      tmp = tmp\
        .withColumn(col, f.round(f.col(col), 0).cast('integer'))\
        .withColumn(col, f.expr("date_add('1960-01-01', " + col + ")"))
      
  for col in [col for col in tmp.columns if col in ['n_rows', 'n']]:    
    tmp = tmp\
      .withColumn(col, f.format_number(f.col(col), 0))
  
  if(byvar == 'all1'):
    tmp = tmp\
      .drop(byvar)
  
  cols = tmp.columns
  tmp = tmp\
    .withColumn('var', f.lit(f'{var}'))\
    .select('var', *cols)
    
  pd.set_option('display.max_rows', tmp.count())
  pd.set_option('display.max_columns', len(tmp.columns))
  pd.set_option('display.width', 1000)
  pd.set_option('expand_frame_repr', False)   
  print(tmp.toPandas(), flush=True)
  pd.reset_option('display.max_rows')
  pd.reset_option('display.max_columns')
  pd.reset_option('expand_frame_repr')
  
  return tmp


# COMMAND ----------

def rename_columns(df, columns):
  print('rename_columns...', flush=True)
  if isinstance(columns, dict):
    for old_name, new_name in columns.items():
      print("  Renaming " + old_name + " to " + new_name, flush=True)
      df = df.withColumnRenamed(old_name, new_name)
    return df
  else:
    raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

# COMMAND ----------

# zw comment: As we are using both spark and pandas dataframes, we should help ourselves by explicit defining each type
# zw comment: suggest from pyspark.sql import DataFrame as SparkDataFrame
# zw comment: suggest from pandas import DataFrame as PandasDataFrame.

from pyspark.sql import DataFrame
from typing import Any, Callable, List, Mapping, Optional, Tuple

def merge(df1: DataFrame, df2: DataFrame, ilist, quietly=0, broadcast_right=0, validate=None, assert_results=None, keep_results=None, indicator=1) -> DataFrame:
  
  import pyspark.sql.functions as f  
  
  ilists = ', '.join(ilist) 
  if(quietly == 0): 
    df1name = [x for x in globals() if globals()[x] is df1]
    if(len(df1name) == 1): df1name = df1name[0]
    else: df1name = "unknown (not in globals())"    
    df2name = [x for x in globals() if globals()[x] is df2]
    if(len(df2name) == 1): df2name = df2name[0]
    else: df2name = "unknown (not in globals())"  
    # print('\nmerge', flush=True)
    print("merge(df1=" + df1name + "; df2=" + df2name + "; ilist=" + ilists + ")", flush=True)
  
  # validate
  if(validate not in ['1:1', '1:m', 'm:1', 'm:m', None]):
    raise ValueError(f"'validate' was passed {validate}, but should take values: '1:1', '1:m', 'm:1', 'm:m', or None.")    
  if(validate in ['1:1', '1:m']):
    count1 = df1.count()
    count1d = df1.dropDuplicates(ilist).count()
    assert count1 == count1d
    print(f'check_id: df1 - {df1name} ({ilists}) is unique (N = {count1:,})')
  if(validate in ['1:1', 'm:1']):
    count2 = df2.count()
    count2d = df2.dropDuplicates(ilist).count()
    assert count2 == count2d
    print(f'check_id: df2 - {df2name} ({ilists}) is unique (N = {count2:,})')
  if(validate in ['1:1', '1:m', 'm:1']):
    pass # print()
  
  
  # check for common variables
  vlist1 = [v for v in df1.columns if v not in ilist]
  vlist2 = [v for v in df2.columns if v not in ilist]
  vlista = [v for v in vlist1 if v not in vlist2]
  vlistb = [v for v in vlist2 if v not in vlist1]
  vlistc = [v for v in vlist1 if v in vlist2]  
  vlista_len = len(vlista)
  vlistb_len = len(vlistb)
  vlistc_len = len(vlistc)
  if(len(vlistc) > 0): 
    print(f" * * Warning: Common variables found ({vlistc_len})" + "= " + str(vlistc))  
  
  # add indicator vars
  df1 = df1\
    .withColumn("in_left", f.lit(1))
  df2 = df2\
    .withColumn("in_right", f.lit(1))
  
  # merge
  if(broadcast_right == 0):
    tmp = df1\
      .join(df2, ilist, 'outer') 
  elif(broadcast_right == 1):
    print('broadcast_right')
    tmp = df1\
      .join(f.broadcast(df2), ilist, 'outer') 
  else: ValueError("'broadcast_right' should take values: 0 or 1") 
    
  # add _merge var
  tmp = tmp\
    .withColumn("_merge",\
      f.when(f.col("in_left").isNotNull() & f.col("in_right").isNotNull(), "both")\
      .when(f.col("in_left").isNotNull(), "left_only")\
      .when(f.col("in_right").isNotNull(), "right_only")\
    )\
    .drop("in_left", "in_right")    
  
  # tab _merge
  if(quietly == 0):
    print('tabulating _merge\n')
    ttt = tab(tmp, '_merge')
  
  # assert results       
  if(assert_results != None):
    if(all(x in ['both', 'left_only', 'right_only'] for x in assert_results)):
      # deduplicate      
      assert_results = list(set(assert_results)) 
      assert tmp.count() == tmp.where(f.col('_merge').isin(assert_results)).count(), 'Comment ZW: Can we add a meaningful message to the user?'
      if(quietly == 0): 
        assert_resultss = ', '.join(assert_results) 
        print(); print(f'assert_results ({assert_resultss}) is satisfied')
    else: 
      raise ValueError("'assert_results' should take values: 'both', 'left_only', 'right_only'")  

  # keep results
  if(keep_results != None):
    if(all(x in ['both', 'left_only', 'right_only'] for x in keep_results)):
      # deduplicate      
      keep_results = list(set(keep_results)) 
      tmp = tmp\
        .where(f.col('_merge').isin(keep_results))

      if(quietly == 0): 
        print(); print(f'post keep_results ({keep_results})')
        ttt = tab(tmp, '_merge')
    else: 
      raise ValueError("'keep_results' should take values: 'both', 'left_only', 'right_only'")  

  # indicator      
  if(indicator == 0):
    tmp = tmp\
      .drop('_merge')
        
  return tmp

# COMMAND ----------

# Source: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
from pyspark.sql import DataFrame
from typing import Iterable

def reshape_wide_to_long(df: DataFrame,
                         i: Iterable[str], j, stubname) -> DataFrame:
  
  from pyspark.sql.functions import array, col, explode, lit, struct

  # NOTE: Need to extend for multiple stubnames...
  
  # create list of columns matching stubname that are to be reshaped from wide to long
  _cols = [v for v in list(df.columns) if re.match(r'^(' + stubname + ')(.*)$', v)]

  # create array of struct
  _wide_to_long = f.array(*[f.struct(lit(re.match(r'^(' + stubname + ')(.*)$', v).group(2)).alias(j), col(v).alias(stubname)) for v in _cols])

  # reshape
  _tmp = df.withColumn("widetolong", explode(_wide_to_long))\
    .select(*(i + [col("widetolong")[v].alias(v) for v in [j, stubname]]))

  return _tmp


# COMMAND ----------

# Source: https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe
from pyspark.sql import DataFrame
from typing import Iterable

def reshape_wide_to_long_multi(df: DataFrame,
                         i: Iterable[str], j, stubnames: Iterable[str]) -> DataFrame:
  
  from pyspark.sql.functions import array, col, explode, lit, struct

  # create df of columns matching stubname that are to be reshaped from wide to long  
  # check stubnames have consistent j
  _tmpm = []
  for ia, stubname in enumerate(stubnames):
    _cols = [col for col in list(df.columns) if re.match(r'^(' + stubname + ')(.*)$', col)]
    _tmp = pd.DataFrame({f'_cols_{ia}': _cols})
    _tmp['j'] = _tmp[f'_cols_{ia}'].str.extract(r'^' + stubname + '(.*)$')

    if(ia == 0): 
      _tmpm = _tmp
      _tmpm = _tmp[['j', '_cols_0']]
    else: 
      _tmpm = pd.merge(_tmpm, _tmp, on='j', how='outer', validate="one_to_one", indicator=True)
      assert (_tmpm['_merge'] == 'both').all()
      _tmpm = _tmpm\
        .drop(['_merge'], axis=1)  

  # create array of struct
  _wide_to_long = f.array(\
    *[
      f.struct(\
        f.lit(_tmpm.iloc[ib]['j']).alias(j)\
        , *[f.col(_tmpm.iloc[ib][f'_cols_{ia}']).alias(stubname) for ia, stubname in enumerate(stubnames)]\
      ) for ib in _tmpm.index
    ]\
  )

  # reshape  
  _tmpf = df\
    .withColumn("widetolong", f.explode(_wide_to_long))\
    .select(*(i + [f.col("widetolong")[col].alias(col) for col in [j, *[stubname for stubname in stubnames]]]))  
    
  return _tmpf

# COMMAND ----------

def reshape_long_to_wide(df, i, j, stubname):

  import pyspark.sql.functions as f 
  
  # NOTE: Need to extend for multiple stubnames...

  # check ID variables to ensure later use of first is appropriate
  vlist = i.copy()
  vlist.append(j)
  count1 = df.count()
  count2 = df.select(vlist).dropDuplicates().count()
  assert count1 == count2
  
  # check j
  jrows = df.select(j).dropDuplicates().orderBy(j).collect()
  jlist = [row[0] for row in jrows]
  print("  jlist = " + str(jlist), flush=True)
  
  # reshape (use first knowing that i are unique for j, so only one record to take)
  tmp = df.groupBy(i).pivot(j).agg(f.first(f.col(stubname))).orderBy(i)

  # rename columns
  vlist = [v for v in tmp.columns if v not in i]
  print("  vlist = " + str(vlist), flush=True)
  vlist.sort(key=float)
  print("  vlist(sorted) = " + str(vlist), flush=True)  
  dlist = {v : stubname + v for v in vlist}
  tmp = rename_columns(tmp, dlist)  
  
  # reorder columns
  rlist = i.copy()
  vlist = [stubname + v for v in vlist]
  rlist = rlist + vlist
  tmp = tmp.select(rlist)
  
  return tmp

# COMMAND ----------

def check_id(df, ilist, warningError=None):
  dfname = [x for x in globals() if globals()[x] is df][0]
  ilists = ', '.join(ilist)
  # print('check_id', flush=True)
  # print("  df=" + dfname + "; ilist=" + ilists + "; warningErorr=" + str(warningError) + ".", flush=True)

  count1 = df.count()
  count2 = df.select(ilist).dropDuplicates().count()

  if(count1 == count2):
    print(f'check_id: {dfname} ({ilists}) is unique (N = {count1:,d})', flush=True)
  else:
    if(warningError == None):
      print("  WARNING: ilist is NOT unique (count() = " + f'{count1:,d}' + ", dropDuplciates().count() = " + f'{count2:,d}' + ").", flush=True)
    elif(warningError == 1):
      raise Exception("  ERROR: ilist is NOT unique (count() = " + f'{count1:,d}' + ", dropDuplciates().count() = " + f'{count2:,d}' + ").")
    else:
      raise Exception("  ERROR: warningError not recognised.")        

# COMMAND ----------

def check_null(df, ilist, warningError):
  
  import pyspark.sql.functions as f 
  import pandas as pd
  import numpy as np
    
  dfname = [x for x in globals() if globals()[x] is df][0]
  ilists = ', '.join(ilist)
  # print(f'check_null: {dfname} ({ilists})', flush=True)
  # print("  df=" + dfname + "; ilist=" + ilists + "; warningErorr=" + str(warningError) + ".", flush=True)

  tmp = df.select(ilist)

  # count nulls
  tmp1 = tmp.select([f.count(f.when(f.isnull(v), v)).alias(v) for v in tmp.columns]) 
  tmp1 = tmp1.toPandas().rename(index={0: 'null'}).T

  # count NaNs
  indnan = 0
  if(all([vartype in ('timestamp', 'string', 'date') for (v,vartype) in tmp.dtypes])): 
    indnan = 1
    # print("  ilist is ('timestamp', 'string', 'date') - not counting NaNs.", flush=True)
    tmp2 = tmp1.drop(columns=['null'])
    tmp2['nan'] = np.nan
  else:     
    tmp2 = tmp.select([f.count(f.when(f.isnan(v), v)).alias(v) for (v,vartype) in tmp.dtypes if vartype not in ('timestamp', 'string', 'date')]) 
    tmp2 = tmp2.toPandas().rename(index={0: 'nan'}).T

  # merge
  tmp = tmp1.join(tmp2, how='outer')
  
  tmp = tmp.loc[ilist] # reorder
  tmp = tmp.rename_axis('varname').reset_index() # index as column
  tmp = tmp.replace({np.nan: -1}) # NaN to -1
  tmp['nan'] = tmp['nan'].astype(int) # convert to integer
  tmp = tmp.replace({-1: None}) # -1 to None
  tmp.loc['Total'] = tmp.sum()
  tmp.loc['Total', 'varname'] = ''

  ind = 0
  if(tmp.loc['Total', 'null'] == 0):
    print(f'check_null: {dfname} ({ilists}) has no null values', flush=True)
  else:
    ind = 1
    print('', flush=True) 
    print(tmp, flush=True)
    print('', flush=True)
    
    if(warningError == 0):
      print("  WARNING: ilist has null values", flush=True)
    elif(warningError == 1):
      raise Exception("  ERROR: ilist has null values")
    else:
      raise Exception("  ERROR: warningError not recognised.") 
 
  if(tmp.loc['Total', 'nan'] == 0):
    if(indnan == 0): 
      ggg = 1 # print("  ilist has no NaN values", flush=True)
  else:
    if(ind == 0):
      print('', flush=True) 
      print(tmp)
      print('', flush=True) 
      
    if(warningError == 0):
      print("  WARNING: ilist has NaN values", flush=True)
    elif(warningError == 1):
      raise Exception("  ERROR: ilist has NaN values")
    else:
      raise Exception("  ERROR: warningError not recognised.")        

# COMMAND ----------

# https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/968100988546031/764888478920499/8836542754149149/latest.html
def null_safe_equality(a, b):
  return a == b
 
import pyspark.sql.functions as f 
import pyspark.sql.types as t
udf_null_safe_equality = f.udf(null_safe_equality, t.BooleanType())

# and then apply it directly to the dataframe
# pyspark_null_results = null_df\
#   .withColumn("standard_equality", f.col('operand_1') == f.col('operand_2'))\
#   .withColumn("null_equality", udf_null_safe_equality('operand_1', 'operand_2'))
 

# COMMAND ----------

def compare_files(df1, df2, ilist, warningError=None):
  df1name = [x for x in globals() if globals()[x] is df1][0]
  df2name = [x for x in globals() if globals()[x] is df2][0]
  ilists = ', '.join(ilist)
  print('--------------------------------------------------------------------------------------', flush=True)
  print('compare_files', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  # print("  df1=" + df1name + "; df2=" + df2name + "; ilist=" + ilists + "; warningErorr=" + str(warningError) + ".", flush=True)

  # check i variables are in df1 and df2
  # print(f'check {ilists} is in {df1name} and {df2name}', flush=True)
  if(not (all([v in df1.columns for v in ilist]))): raise Exception("  ERROR: All ilist not in df1.") 
  if(not (all([v in df2.columns for v in ilist]))): raise Exception("  ERROR: All ilist not in df2.") 
  
  # check ID 
  check_id(df1, ilist, warningError=1)
  check_id(df2, ilist, warningError=1)
  
  # check null (null values do not join without using <=> [null safe equality operator], alternatively replace nulls with other values beforehand)
  check_null(df1, ilist, warningError=1)
  check_null(df2, ilist, warningError=1)

  
  # ------------------------------------------------------------------------------
  # check variables to be compared
  # ------------------------------------------------------------------------------
  #print('', flush=True)
  print('check common variables:', flush=True)
  vlist1 = [v for v in df1.columns if v not in ilist]
  vlist2 = [v for v in df2.columns if v not in ilist]
  vlista = [v for v in vlist1 if v not in vlist2]
  vlistb = [v for v in vlist2 if v not in vlist1]
  vlistc = [v for v in vlist1 if v in vlist2]  
  vlista_len = len(vlista)
  vlistb_len = len(vlistb)
  vlistc_len = len(vlistc)
  
  tm1 = df1name + " only (" + str(vlista_len) + ")"
  tm2 = df2name + " only (" + str(vlistb_len) + ")"
  tm3 = "both (" + str(vlistc_len) + ")"
  
  # tmc = len(str(f'{tm3} ')) 
  tmc = len(' ' + f'{str(max(tm1, tm2))}') 
  if(len(vlist1) > 0): print("  " + f"{tm1:<{tmc}}" + "= " + str(vlista), flush=True)
  if(len(vlist2) > 0): print("  " + f"{tm2:<{tmc}}" + "= " + str(vlistb), flush=True)
  if(len(vlistc) > 0): print("  " + f"{tm3:<{tmc}}" + "= " + str(vlistc), flush=True)
        
  #if(len(vlist2) > 0): print(f'  {df2name}: ' + str(vlistb), flush=True)
  #if(len(vlistc) > 0): print(f'  {df1name} & {df2name}: ' + str(vlistc), flush=True)
  
  
  # ------------------------------------------------------------------------------
  # check common observations
  # ------------------------------------------------------------------------------  
  # prepare files for merge 
  # select ilist and common variables to be compared
  vlist = ilist + vlistc
  df1 = df1.select(vlist)
  df2 = df2.select(vlist)   
  # rename variables _ONE, _TWO
  df1 = df1.select(ilist + [f.col(v).alias(v + "_ONE") for v in df1.columns if v not in ilist])
  df2 = df2.select(ilist + [f.col(v).alias(v + "_TWO") for v in df2.columns if v not in ilist])   
    
  # merge
  # NOTE: assuming no nulls in ilist - any row with a null value in the ilist variables is dropped in pyspark!

  print(f'check common observations:', flush=True)
  df1 = df1.withColumn("in_ONE", f.lit(1))
  df2 = df2.withColumn("in_TWO", f.lit(1)) 
  
  # https://stackoverflow.com/questions/45713290/how-to-resolve-the-analysisexception-resolved-attributes-in-spark/53848160
  df2 = df2.select([f.col(v).alias(v) for v in df2.columns]) # hack to avoid 
  
  #df1.limit(5).show()
  #df2.limit(5).show()
  #print('here1', flush=True)
  #global dfm
  dfm = df1.join(df2, ilist, "outer").withColumn("_merge", \
      f.when((f.col("in_ONE").isNotNull()) & (f.col("in_TWO").isNotNull()), 3)\
      .when(f.col("in_ONE").isNotNull(), 1)\
      .when(f.col("in_TWO").isNotNull(), 2)\
      )\
      .drop("in_ONE", "in_TWO")\
      .orderBy(ilist)
  #print('here2', flush=True)
  # print('', flush=True)
  # ttt = tab(dfm, '_merge')
  file1 = dfm.where(f.col("_merge") == 1).drop("_merge")
  file2 = dfm.where(f.col("_merge") == 2).drop("_merge")
  file3 = dfm.where(f.col("_merge") == 3).drop("_merge")
  count_file1 = file1.count()
  count_file2 = file2.count()
  count_file3 = file3.count()
  count_filem = max(count_file1, count_file2, count_file3)
  tmm = len(' ' + f'{count_filem:,}')
  
  tm1 = df1name + " only)"
  tm2 = df2name + " only)"
  tm3 = "both)"
  tmc = tmc + 1
  print("  file1 (" + f"{tm1:<{tmc}}" + "=" + f'{count_file1:{tmm},d}', flush=True)
  print("  file2 (" + f"{tm2:<{tmc}}" + "=" + f'{count_file2:{tmm},d}', flush=True)
  print("  file3 (" + f"{tm3:<{tmc}}" + "=" + f'{count_file3:{tmm},d}', flush=True)
  #print(f'  {df2name}: {count_file2}', flush=True)
  #print(f'  {df1name} & {df2name}: {count_file3}', flush=True)
  
  # ------------------------------------------------------------------------------
  # compare
  # ------------------------------------------------------------------------------
  # NOTE: null safe equality (!= would exclude any rows with a null value in variable being compared)
  print('compare common variables for common observations (file3) using null_safe_eqaulity, ', flush=True)  
  print('  differences:', flush=True)  
  ind = 0
  tmf = max([len(v) for v in vlistc]) + 1
  for v in vlistc:
    tlist = [v + "_ONE", v + "_TWO"]
    tmp = file3\
      .select(ilist + tlist)\
      .withColumn("null_equality", udf_null_safe_equality(v + "_ONE", v + "_TWO"))\
      .where(~f.col("null_equality"))
    tmpc = tmp.count()
    print("  " + f"{v:<{tmf}}" + "=" + f'{tmpc:10,d}', flush=True)
    if(tmpc > 0):
      ind = ind + 1
      
      tmp = tmp\
        .withColumn("varname", f.lit(v))\
        .withColumn("varnum", f.lit(ind))\
        .withColumnRenamed(v + "_ONE", "_ONE")\
        .withColumnRenamed(v + "_TWO", "_TWO")\
        .select(ilist + ["varnum", "varname", "_ONE", "_TWO"]) 
      
      # convert date to string (added 20220521)
      dt1 = dict(tmp.dtypes)["_ONE"]
      dt2 = dict(tmp.dtypes)["_TWO"]  
      if(dt1 != dt2): print(f'    ERROR: dt1 = {dt1}, dt2 = {dt2}')
      assert dt1 == dt2
      if(dt1 in ['date']): 
        tmp = tmp\
          .withColumn("_ONE", f.date_format('_ONE', "yyyy-MM-dd"))\
          .withColumn("_TWO", f.date_format('_TWO', "yyyy-MM-dd"))
      if(re.match('^array.*', dt1)): 
        tmp = tmp\
          .withColumn("_ONE", f.col('_ONE').cast(t.StringType()))\
          .withColumn("_TWO", f.col('_TWO').cast(t.StringType()))
      
      if(ind > 1): tmp = compare_files.union(tmp)
      compare_files = tmp
      
  if(ind == 0): 
    print("\n  *** common variables are the same ***", flush=True)
    compare_files = []
  else:   
    # print('', flush=True)
    # tmp = tab(compare_files, 'varname')
    compare_files = compare_files.orderBy(ilist + ["varnum"])
    if(compare_files.count() <= 50): 
      compare_files.show(truncate = False)
      # print(compare_files.toPandas().to_string())

  print('--------------------------------------------------------------------------------------', flush=True)      
  return file1, file2, file3, compare_files 
  # return compare_files

# COMMAND ----------

def sdc(_df, _vlist):
  # rewrite in pandas when time
  
  print('--------------------------------------------------------------------------------------', flush=True)
  print('sdc (0 < x < 10)', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  
  _df_name = [x for x in globals() if globals()[x] is _df][0]
  print(_df_name); print()
  
  _df.cache()
  print(f'  N(rows) = {_df.count():,}')
  print(f'  N(variables to check) = {len(_vlist)}'); print()
      
  for i, v in enumerate(_vlist):
    ttype = dict(_df.dtypes)[v]
    print(' ', i, v, '(' + ttype + ')')
    assert ttype in ['bigint', 'int', 'long']
    _df = _df\
      .withColumn(v + '_str', f.col(v).cast(t.StringType()))\
      .withColumn(v + '_str_sdc', f.when((f.col(v) > 0) & (f.col(v) < 10), '<10').otherwise(f.col(v + '_str')))\
      .withColumn('_nse', udf_null_safe_equality(v + '_str', v + '_str_sdc').cast(t.IntegerType()))\
      .withColumn('_chk', f.when(f.col(v) < 0, 1))
    _n_replacements = _df.where(f.col('_nse') == 0).count()
    print(f'    {_n_replacements:,} replacements')
    assert _df.select('_chk').where(f.col('_chk') == 1).count() == 0
    _df = _df\
      .drop(v, v + '_str', '_nse', '_chk')\
      .withColumnRenamed(v + '_str_sdc', v)

  print(f'\n  Variables not checked = ' + str([v for v in _df.columns if v not in _vlist]))
  print(f'\n  *** Remember to check across rows ***')
  print('--------------------------------------------------------------------------------------', flush=True)

  return _df

# COMMAND ----------

def code_match(_dict, _name_prefix):
  warnings.warn("\n\nFunction has been deprecated, please use the function codelist_match() instead.", DeprecationWarning )

# COMMAND ----------

def codelist_match(_dict, _name_prefix, _last_event=0, broadcast:int = 0):

  print('--------------------------------------------------------------------------------------', flush=True)
  print('codelist_match', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  print(f'_name_prefix = {_name_prefix}'); print()
  
  # initialise
  _dict_codelist = {}
  _dict_codematch = {}
  
  # loop over data sources
  for i, key in enumerate(_dict):
    # extract elements from the dictionary entry 
    # (dataset, codelist, ordering [in the event of tied matches on date from different sources])
    _data = _dict[key][0]
    _codelist = _dict[key][1]
    _order = _dict[key][2]
    print(i, key, _data, _codelist, _order)

    # get codelist
    _tmp_codelist = globals()[_codelist]\
      .select(['code', 'name'])
    
    # ------------------------------------------------------------------------------------
    # match the dataset and codelist
    # ------------------------------------------------------------------------------------
    print(f'  codelist match')
    if(broadcast == 0):
      # without broadcast
      _tmp_codematch = globals()[_data]\
        .join(_tmp_codelist, on='code', how='inner')
    elif(broadcast == 1):
      # with broadcast
      _tmp_codematch = globals()[_data]\
        .join(f.broadcast(_tmp_codelist), on='code', how='inner')
    else: 
      raise ValueError(f"'broadcast' should take values: 0 or 1. The value provided was {broadcast}") 
    
    # add source and order
    _tmp_codematch = _tmp_codematch\
      .withColumn('source', f.lit(key))\
      .withColumn('sourcen', f.lit(_order))
    
    # store the results
    _dict_codelist[key] = _tmp_codelist
    _dict_codematch[key] = _tmp_codematch
  
  # append
  print(f'\nappend codelist matches from different sources/terminologies')
  _codelist_all = reduce(DataFrame.unionByName, _dict_codelist.values())  
  _codematch_all = reduce(DataFrame.unionByName, _dict_codematch.values()) 
  _dict_codematch['all'] = _codematch_all
  
  # ------------------------------------------------------------------------------------
  # first/last event of each name
  # ------------------------------------------------------------------------------------
  if(_last_event == 1):
    print(f'filter to LAST event')
    _win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy(f.desc('DATE'), 'sourcen', 'code')      
  else:
    print(f'filter to 1st event')
    _win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy('DATE', 'sourcen', 'code')  
  
  # filter
  _codematch_1st = _codematch_all\
    .withColumn('_rownum', f.row_number().over(_win))\
    .where(f.col('_rownum') == 1)\
    .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
    .orderBy('PERSON_ID', 'DATE', 'name')

  # ------------------------------------------------------------------------------------
  # identify ties by source for first event of each name
  # ------------------------------------------------------------------------------------
  print(f'identify ties from sources/terminologies for first (/last) event')
  
  _codematch_1st_tie_source = _codematch_all\
    .withColumn('_tie', f.dense_rank().over(_win))\
    .where(f.col('_tie') == 1)\
    .groupBy('PERSON_ID', 'name')\
    .agg(\
      f.countDistinct(f.col('source')).alias(f'_n_distinct_source')\
      , f.countDistinct(f.when(f.col('source').isNull(), 1)).alias(f'_null_source')\
      , f.sort_array(f.collect_set(f.col('source'))).alias('_tie_source_list')\
    )\
    .withColumn('_tie_source', f.when((f.col('_n_distinct_source') + f.col(f'_null_source')) > 1, 1).otherwise(0))\
    .select('PERSON_ID', 'name', '_tie_source', '_tie_source_list')
  
  _codematch_1st = _codematch_1st\
    .join(_codematch_1st_tie_source, on=['PERSON_ID', 'name'], how='left')

  # ------------------------------------------------------------------------------------
  # reshape
  # ------------------------------------------------------------------------------------
  print(f'reshape long to wide')
  
  # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
  _codelist_all = _codelist_all\
    .select('name')\
    .distinct() 

  # reshape long to wide  
  _codematch_1st_wide = _codematch_1st\
    .drop('code')\
    .join(_codelist_all, on='name', how='outer')\
    .withColumn('name', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('name'))))\
    .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
    .pivot('name')\
    .agg(f.first('DATE'))\
    .where(f.col('PERSON_ID').isNotNull())\
    .orderBy('PERSON_ID')  
    
  # add flag and date columns
  print(f'add flag and date')
  vlist = []
  for i, v in enumerate([col for col in list(_codematch_1st_wide.columns) if re.match(f'^{_name_prefix}', col)]):
    print(' ' , i, v)
    _codematch_1st_wide = _codematch_1st_wide\
      .withColumnRenamed(v, v + '_date')\
      .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
    vlist = vlist + [v + '_flag', v + '_date']
  _codematch_1st_wide = _codematch_1st_wide\
    .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist)    
    
  return _dict_codematch, _codematch_1st, _codematch_1st_wide

# COMMAND ----------

# 20230126 testing option for stages to run
def codelist_match_stages_to_run(_dict, _name_prefix, _last_event=0, broadcast:int = 0, stages_to_run:int = 3):

  print('--------------------------------------------------------------------------------------', flush=True)
  print('codelist_match', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  print(f'_name_prefix = {_name_prefix}'); print()
  
  # check stages_to_run
  assert stages_to_run in [1,2,3], 'stages_to_run is not in [1,2,3]'
  
  # initialise
  _dict_codelist = {}
  _dict_codematch = {}
  
  # loop over data sources
  for i, key in enumerate(_dict):
    # extract elements from the dictionary entry 
    # (dataset, codelist, ordering [in the event of tied matches on date from different sources])
    _data = _dict[key][0]
    _codelist = _dict[key][1]
    _order = _dict[key][2]
    print(i, key, _data, _codelist, _order)

    # get codelist
    _tmp_codelist = globals()[_codelist]\
      .select(['code', 'name'])
    
    # ------------------------------------------------------------------------------------
    # match the dataset and codelist
    # ------------------------------------------------------------------------------------
    print(f'  codelist match')
    if(broadcast == 0):
      # without broadcast
      _tmp_codematch = globals()[_data]\
        .join(_tmp_codelist, on='code', how='inner')
    elif(broadcast == 1):
      # with broadcast
      _tmp_codematch = globals()[_data]\
        .join(f.broadcast(_tmp_codelist), on='code', how='inner')
    else: 
      raise ValueError(f"'broadcast' should take values: 0 or 1. The value provided was {broadcast}") 
    
    # add source and order
    _tmp_codematch = _tmp_codematch\
      .withColumn('source', f.lit(key))\
      .withColumn('sourcen', f.lit(_order))
    
    # store the results
    _dict_codelist[key] = _tmp_codelist
    _dict_codematch[key] = _tmp_codematch
  
  # append
  print(f'\nappend codelist matches from different sources/terminologies')
  _codelist_all = reduce(DataFrame.unionByName, _dict_codelist.values())  
  _codematch_all = reduce(DataFrame.unionByName, _dict_codematch.values()) 
  _dict_codematch['all'] = _codematch_all
  
  
  # ------------------------------------------------------------------------------------
  # first/last event of each name
  # ------------------------------------------------------------------------------------
  if(stages_to_run in [2,3]):
    if(_last_event == 1):
      print(f'filter to LAST event')
      _win = Window\
        .partitionBy(['PERSON_ID', 'name'])\
        .orderBy(f.desc('DATE'), 'sourcen', 'code')      
    else:
      print(f'filter to 1st event')
      _win = Window\
        .partitionBy(['PERSON_ID', 'name'])\
        .orderBy('DATE', 'sourcen', 'code')  

    # filter
    _codematch_1st = _codematch_all\
      .withColumn('_rownum', f.row_number().over(_win))\
      .where(f.col('_rownum') == 1)\
      .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
      .orderBy('PERSON_ID', 'DATE', 'name')

    # ------------------------------------------------------------------------------------
    # identify ties by source for first event of each name
    # ------------------------------------------------------------------------------------
    print(f'identify ties from sources/terminologies for first (/last) event')

    _codematch_1st_tie_source = _codematch_all\
      .withColumn('_tie', f.dense_rank().over(_win))\
      .where(f.col('_tie') == 1)\
      .groupBy('PERSON_ID', 'name')\
      .agg(\
        f.countDistinct(f.col('source')).alias(f'_n_distinct_source')\
        , f.countDistinct(f.when(f.col('source').isNull(), 1)).alias(f'_null_source')\
        , f.sort_array(f.collect_set(f.col('source'))).alias('_tie_source_list')\
      )\
      .withColumn('_tie_source', f.when((f.col('_n_distinct_source') + f.col(f'_null_source')) > 1, 1).otherwise(0))\
      .select('PERSON_ID', 'name', '_tie_source', '_tie_source_list')

    _codematch_1st = _codematch_1st\
      .join(_codematch_1st_tie_source, on=['PERSON_ID', 'name'], how='left')

  # ------------------------------------------------------------------------------------
  # reshape
  # ------------------------------------------------------------------------------------
  if(stages_to_run == 3):
    print(f'reshape long to wide')

    # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
    _codelist_all = _codelist_all\
      .select('name')\
      .distinct() 

    # reshape long to wide  
    _codematch_1st_wide = _codematch_1st\
      .drop('code')\
      .join(_codelist_all, on='name', how='outer')\
      .withColumn('name', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('name'))))\
      .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
      .pivot('name')\
      .agg(f.first('DATE'))\
      .where(f.col('PERSON_ID').isNotNull())\
      .orderBy('PERSON_ID')  

    # add flag and date columns
    print(f'add flag and date')
    vlist = []
    for i, v in enumerate([col for col in list(_codematch_1st_wide.columns) if re.match(f'^{_name_prefix}', col)]):
      print(' ' , i, v)
      _codematch_1st_wide = _codematch_1st_wide\
        .withColumnRenamed(v, v + '_date')\
        .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
      vlist = vlist + [v + '_flag', v + '_date']
    _codematch_1st_wide = _codematch_1st_wide\
      .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist)    
    
  # ------------------------------------------------------------------------------------
  # return
  # ------------------------------------------------------------------------------------    
  if(stages_to_run == 1): return _dict_codematch  
  elif(stages_to_run == 2): return _dict_codematch, _codematch_1st  
  elif(stages_to_run == 3): return _dict_codematch, _codematch_1st, _codematch_1st_wide

# COMMAND ----------

# 20230102 - testing different union method to allow more columns to be retained
# 20230107 - testing dict of dict (cf. dict of list) input

def codelist_match_v2_test(_dict, _name_prefix, _last_event=0, broadcast=0, codelist_cols=None, stages_to_run:int = 3):

  print('--------------------------------------------------------------------------------------', flush=True)
  print('codelist_match', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  print(f'_name_prefix = {_name_prefix}'); print()
  
  # check stages_to_run
  assert stages_to_run in [1,2,3], 'stages_to_run is not in [1,2,3]'  
  
  # initialise
  _dict_codelist = {}
  _dict_codematch = {}
  
  # loop over data sources
  for i, key in enumerate(_dict):
    # extract elements from the dictionary entry 
    # (dataset, codelist, ordering [in the event of tied matches on date from different sources])
    _data = _dict[key][0]
    _codelist = _dict[key][1]
    _order = _dict[key][2]
    print(i, key, _data, _codelist, _order)

    # get codelist
    _codelist_cols = ['code', 'name']
    if(codelist_cols != None): _codelist_cols = _codelist_cols + codelist_cols
    _tmp_codelist = globals()[_codelist]\
      .select(_codelist_cols)
    
    # ------------------------------------------------------------------------------------
    # match the dataset and codelist
    # ------------------------------------------------------------------------------------
    print(f'  codelist match')
    if(broadcast == 0):
      # without broadcast
      _tmp_codematch = globals()[_data]\
        .join(_tmp_codelist, on='code', how='inner')
    elif(broadcast == 1):
      # with broadcast
      _tmp_codematch = globals()[_data]\
        .join(f.broadcast(_tmp_codelist), on='code', how='inner')
    else: ValueError("'broadcast' should take values: 0 or 1") 
    
    # add source and order
    _tmp_codematch = _tmp_codematch\
      .withColumn('source', f.lit(key))\
      .withColumn('sourcen', f.lit(_order))
    
    # store the results
    _dict_codelist[key] = _tmp_codelist
    _dict_codematch[key] = _tmp_codematch
  
    # append for all
    if(i == 0):
      _codematch_all = _tmp_codematch
    else:
      # before union
      # add columns to the master that are in the component but not in the master
      for col in [col for col in _tmp_codematch.columns if col not in _codematch_all.columns]:
        print('  adding column found in tmp to master: ' + col)
        _codematch_all = _codematch_all\
          .withColumn(col, f.lit(None))
      # add columns to the component that are in the master but not in the component
      for col in [col for col in _codematch_all.columns if col not in _tmp_codematch.columns]:
        print('  adding column found in master to tmp: ' + col)
        _tmp_codematch = _tmp_codematch\
          .withColumn(col, f.lit(None))

      # union  
      _codematch_all = _codematch_all\
        .unionByName(_tmp_codematch)
  
  # append
  print(f'\nappend codelist matches from different sources/terminologies')
  _codelist_all = reduce(DataFrame.unionByName, _dict_codelist.values())  
  # commented out the line below as undertaken manually above to allow flexibility on appending new columns
  # _codematch_all = reduce(DataFrame.unionByName, _dict_codematch.values())   
  _dict_codematch['all'] = _codematch_all
  
  if(stages_to_run == 3):

    # ------------------------------------------------------------------------------------
    # first/last event of each name
    # ------------------------------------------------------------------------------------
    if(stages_to_run in [2,3]):
      if(_last_event == 1):
        print(f'filter to LAST event')
        _win = Window\
          .partitionBy(['PERSON_ID', 'name'])\
          .orderBy(f.desc('DATE'), 'sourcen', 'code')      
      else:
        print(f'filter to 1st event')
        _win = Window\
          .partitionBy(['PERSON_ID', 'name'])\
          .orderBy('DATE', 'sourcen', 'code')  

      # filter
      _codematch_1st = _codematch_all\
        .withColumn('_rownum', f.row_number().over(_win))\
        .where(f.col('_rownum') == 1)\
        .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
        .orderBy('PERSON_ID', 'DATE', 'name')

      # ------------------------------------------------------------------------------------
      # identify ties by source for first event of each name
      # ------------------------------------------------------------------------------------
      print(f'identify ties from sources/terminologies for first (/last) event')

      _codematch_1st_tie_source = _codematch_all\
        .withColumn('_tie', f.dense_rank().over(_win))\
        .where(f.col('_tie') == 1)\
        .groupBy('PERSON_ID', 'name')\
        .agg(\
          f.countDistinct(f.col('source')).alias(f'_n_distinct_source')\
          , f.countDistinct(f.when(f.col('source').isNull(), 1)).alias(f'_null_source')\
          , f.sort_array(f.collect_set(f.col('source'))).alias('_tie_source_list')\
        )\
        .withColumn('_tie_source', f.when((f.col('_n_distinct_source') + f.col(f'_null_source')) > 1, 1).otherwise(0))\
        .select('PERSON_ID', 'name', '_tie_source', '_tie_source_list')

      _codematch_1st = _codematch_1st\
        .join(_codematch_1st_tie_source, on=['PERSON_ID', 'name'], how='left')

      
    # ------------------------------------------------------------------------------------
    # reshape
    # ------------------------------------------------------------------------------------
    if(stages_to_run == 3):
      print(f'reshape long to wide')

      # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
      _codelist_all = _codelist_all\
        .select('name')\
        .distinct() 

      # reshape long to wide  
      _codematch_1st_wide = _codematch_1st\
        .drop('code')\
        .join(_codelist_all, on='name', how='outer')\
        .withColumn('name', f.concat(f.lit(f'{_name_prefix}'), f.lower(f.col('name'))))\
        .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
        .pivot('name')\
        .agg(f.first('DATE'))\
        .where(f.col('PERSON_ID').isNotNull())\
        .orderBy('PERSON_ID')  

      # add flag and date columns
      print(f'add flag and date')
      vlist = []
      for i, v in enumerate([col for col in list(_codematch_1st_wide.columns) if re.match(f'^{_name_prefix}', col)]):
        print(' ' , i, v)
        _codematch_1st_wide = _codematch_1st_wide\
          .withColumnRenamed(v, v + '_date')\
          .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
        vlist = vlist + [v + '_flag', v + '_date']
      _codematch_1st_wide = _codematch_1st_wide\
        .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist)    

  

  # ------------------------------------------------------------------------------------
  # return
  # ------------------------------------------------------------------------------------    
  # return _dict_codematch, _codematch_1st, _codematch_1st_wide
  if(stages_to_run == 1): return _dict_codematch  
  # elif(stages_to_run == 2): return _dict_codematch, _codematch_1st  
  elif(stages_to_run == 3): return _dict_codematch, _codematch_1st, _codematch_1st_wide

# COMMAND ----------

def code_match_summ(_dict_codelist, _dict_codematch):
  print('function renamed from code_match_summ to codelist_match_summ', flush=True)

# COMMAND ----------

def codelist_match_summ(_dict_codelist, _dict_codematch):
  """
  
  Notes:
  ZW - This need documentation
  """

  _dict_codelist_copy = _dict_codelist.copy()
  _dict_codelist_copy['all'] = ['', '', '']
  
  print('--------------------------------------------------------------------------------------', flush=True)
  print('summarise by name', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)

  _n_list = []
  _out_summ_name = []
  for i, key in enumerate(_dict_codelist_copy):
    print(i, key)
    _out = _dict_codematch[key]
    _codelist = _dict_codelist_copy[key][1]
    print(i, key, _codelist)

     #globals()[_out]\
    _tmp = _out\
      .groupBy('name')\
      .agg(\
        f.count(f.lit(1)).alias(f'n_{key}')\
        , f.countDistinct(f.col('PERSON_ID')).alias(f'n_id_{key}')\
      )
    _n_list = _n_list + [f'n_{key}', f'n_id_{key}']

    if(key != 'all'):
      _tmpc = globals()[_codelist]\
        .select('name')\
        .distinct()
      _tmp = _tmp\
        .join(_tmpc, on='name', how='outer')    

    if(i == 0): _out_summ_name = _tmp
    else:
      _out_summ_name = _out_summ_name\
        .join(_tmp, on='name', how='outer')

  _out_summ_name = _out_summ_name\
    .na.fill(0, subset=_n_list)\
    .orderBy('name')  
  
  print()
  print('--------------------------------------------------------------------------------------', flush=True)
  print('summarise by name and code', flush=True)
  print('--------------------------------------------------------------------------------------', flush=True)
  
  _out_summ_name_code = []
  j = 0
  for i, key in enumerate(_dict_codelist_copy):
    if(key != 'all'):
      
      _out = _dict_codematch[key]
      _codelist = _dict_codelist_copy[key][1]
      print(i, key, _codelist)

      # globals()[_out]
      _tmp = _out\
        .groupBy('name', 'code')\
        .agg(\
          f.count(f.lit(1)).alias(f'n')\
          , f.countDistinct(f.col('PERSON_ID')).alias(f'n_id')\
        )
      _tmpc = globals()[_codelist]\
        .select('name', 'code', 'terminology', 'term')\
        .distinct()
      _tmp = _tmp\
        .join(_tmpc, on=['name', 'code'], how='outer')\
        .withColumn('source', f.lit(f'{key}'))    

      if(j == 0): _out_summ_name_code = _tmp
      else:
        _out_summ_name_code = _out_summ_name_code\
          .unionByName(_tmp)
      j = j + 1
        
  _out_summ_name_code = _out_summ_name_code\
    .na.fill(0, subset=['n', 'n_id'])\
    .orderBy('name', 'source', 'code')\
    .select('name', 'source', 'code', 'terminology', 'term', 'n', 'n_id')

  return _out_summ_name, _out_summ_name_code  

# COMMAND ----------

def table_summ(_path, _name, _convarlist=[], _idvarlist=[]):
  """
  Add what the fucntion does

    Args:
      _path
      _name
      _convarlist=[]
      _idvarlist=[]

    Returns:
      Don't know

    Raises:
      Nothing
  """
  print(_path)
  
  # get table  
  _tmp = spark.table(_path)
  
  # cache
  # _tmp.cache().count()
  
  # TEMPORARY step - restrict to a particular column when developing
  # _tmp = _tmp\
  #  .select('cov_hx_depression_date')
  #  .select('vacc_1st_date', 'vacc_1st_flag') 
  
  # loop over columns
  for i, col in enumerate(_tmp.columns):
    # data type
    dt = dict(_tmp.dtypes)[col]
    print('  ' + str(i) + '/' + str(len(_tmp.columns) - 1) + ' ' + col + ' (' + dt + ')', flush=True)  
    
    _tmp_col = _tmp\
      .select(col)
      # .limit(20) # TEMPORARY step - limit to first 20 rows when developing      
   
    # -------------------------------------------------------------------------------
    # date
    # -------------------------------------------------------------------------------
    if(dt == 'date'):
      # convert date to number of days to reference date
      _tmp_col = _tmp_col\
        .withColumnRenamed(col, col + '_date')\
        .withColumn(col + '_ndays', f.datediff(f.col(col + '_date'), f.to_date(f.lit("1960-01-01"), "yyyy-MM-dd")))

      # summary statistics
      _summstat = _tmp_col\
        .agg(\
          f.count(f.col(col + '_date')).alias('n')\
          , f.sum((f.col(col + '_date').isNull()).cast('int')).alias('n_null')\
          , f.countDistinct(f.col(col + '_date')).alias('n_unique')\
          , f.mean(f.col(col + '_ndays')).alias('mean')\
          , f.stddev(f.col(col + '_ndays')).alias('stddev')\
          , f.min(f.col(col + '_date')).alias('min')\
          , f.max(f.col(col + '_date')).alias('max')\
        )
        # , f.expr('percentile(' + col + '_ndays' + ', 0.025)').alias('p025')\
        # , f.expr('percentile(' + col + '_ndays' + ', 0.25 )').alias('p25')\
        # , f.expr('percentile(' + col + '_ndays' + ', 0.5  )').alias('p50')\
        # , f.expr('percentile(' + col + '_ndays' + ', 0.75 )').alias('p75')\
        # , f.expr('percentile(' + col + '_ndays' + ', 0.975)').alias('p975')\
      
      # reformat 
      _summstat = _summstat\
        .toPandas() 
      varlist = ['mean'] # , 'p025', 'p25', 'p50', 'p75', 'p975']
      for var in varlist:
        _summstat[var] = (pd.to_datetime('1960-01-01') + pd.to_timedelta(_summstat[var], unit="D")).dt.date
        
      if((pd.isna(_summstat['stddev'].min())) & (pd.isna(_summstat['stddev'].max()))):
        print('  stddev nan - values all null')
      else:
        _summstat['stddev'] = _summstat['stddev'].round(1)
      
      # added to preserve formatting when appending below
      _summstat['val'] = ''
      _summstat['freq_n'] = ''
      _summstat['freq_pct'] = ''
      _summstat['freq_note'] = ''      
      
      
    # -------------------------------------------------------------------------------
    # continuous
    # -------------------------------------------------------------------------------      
    elif(col in _convarlist): 
      print('    ' + 'CON', flush=True) 
      
      # summary statistics
      _summstat = _tmp_col\
        .withColumn(col, f.round(f.col(col), 2).cast('double'))\
        .agg(\
          f.count(f.col(col)).alias('n')\
          , f.sum((f.col(col).isNull()).cast('int')).alias('n_null')\
          , f.countDistinct(f.col(col)).alias('n_unique')\
          , f.mean(f.col(col)).alias('mean')\
          , f.stddev(f.col(col)).alias('stddev')\
          , f.min(f.col(col)).alias('min')\
          , f.max(f.col(col)).alias('max')\
        )

      # reformat 
      _summstat = _summstat\
        .toPandas()      
      _summstat['mean'] = _summstat['mean'].round(1)
      _summstat['stddev'] = _summstat['stddev'].round(1)
      
      # added to preserve formatting when appending below
      _summstat['val'] = ''
      _summstat['freq_n'] = ''
      _summstat['freq_pct'] = ''
      _summstat['freq_note'] = ''        
     
    
    # -------------------------------------------------------------------------------
    # categorical
    # -------------------------------------------------------------------------------      
    else:
      if(col in _idvarlist): print('    ' + 'ID', flush=True)       
      
      _summstat = _tmp_col\
        .agg(\
          f.count(f.col(col)).alias('n')\
          , f.sum((f.col(col).isNull()).cast('int')).alias('n_null')\
          , f.countDistinct(f.col(col)).alias('n_unique')\
        )
      _summstat = _summstat\
        .toPandas()\
        .astype(int) 
      
      # 20220816
      # n_distinct = _tmp_col\
      #   .distinct()\
      #   .count()
      n_distinct = _summstat['n_unique'][0]
      if(col in _idvarlist): 
        freq = pd.DataFrame(columns=['val', 'count', 'freq_n', 'freq_pct', 'freq_note'])
      else:
        
        freq = _tmp_col\
          .groupBy(col)\
          .count()\
          .toPandas()\
          .rename(columns={col: 'val', 'count': 'freq_n'})\
          .sort_values('freq_n', ascending=False)
        n_total = freq['freq_n'].sum()
        freq['freq_pct'] = (100 * freq['freq_n'] / n_total).round(1)
        freq = freq.astype({'freq_n': int})
        if(n_distinct < 20): 
          freq = freq.sort_values('val')
          freq['freq_note'] = '' # added to order columns correctly
        else: 
          freq = freq.sort_values('freq_n', ascending=False).head(10)
          freq = freq.astype({'freq_n': int}) # added as needed again to reformat
          freq['freq_note'] = '** top 10 most freq categ only **'
      
      #summstat = pd.merge(summstat.assign(key=1), freq.assign(key=1), on='key', how='outer')
   
      
      ff = 0
      if(col not in _idvarlist): 
        if(\
          ((n_distinct == 1) & (None not in [val[col] for val in _tmp_col.distinct().collect()]))\
          | ((n_distinct == 2) & (None in [val[col] for val in _tmp_col.distinct().collect()]))
        ): 
          pass
#           ff = 1
#           freq = freq[freq['val'].notnull()]
#           freq = freq.reset_index(drop=True)
#           _summstat = pd.merge(_summstat.reset_index(), freq.reset_index(), on=['index'])\
#             .drop('index', axis=1)
        elif((((n_distinct == 0) | (n_distinct == 1)) & (None in [val[col] for val in _tmp_col.distinct().collect()]))):
          pass
#           ff = 1
#           freq = freq.reset_index(drop=True)
#           _summstat = pd.merge(_summstat.reset_index(), freq.reset_index(), on=['index'])\
#             .drop('index', axis=1)
          
      if(ff == 0):
        
        # added to preserve formatting when appending below
        _summstat['val'] = ''
        _summstat['freq_n'] = ''
        _summstat['freq_pct'] = ''
        _summstat['freq_note'] = ''        
        
        freq['n'] = ''
        freq['n_null'] = ''
        freq['n_unique'] = '' 
        
        _summstat = _summstat\
          .append(freq)

      _summstat['val'] = _summstat['val'].fillna('<None>')  
      _summstat['mean'] = ''
      _summstat['stddev'] = ''
      _summstat['min'] = ''
      _summstat['max'] = ''
      _summstat['todrop'] = 0
    
    
    # -------------------------------------------------------------------------------
    # statistical disclosure control
    # -------------------------------------------------------------------------------            
    _summstat = _summstat\
      .reset_index(drop=True)
    for ind in _summstat.index:
      # n counts
      varlist = ['n', 'n_null', 'n_unique']
      # for var in varlist:
        # if(summstat.loc[ind, var] != ''):          
          # if(summstat.loc[ind, var] > 0 & summstat.loc[ind, var] < 5): summstat.loc[ind, var] = '<5'
     
      # frequency counts
      # if(summstat.loc[ind, 'freq_n'] != ''):
        # if(summstat.loc[ind, 'freq_n'] > 0 & summstat.loc[ind, 'freq_n'] < 5):     
          # print(summstat.loc[ind, 'freq_n'])
          # summstat.loc[ind, 'freq_n'] = '<5'
          # summstat.loc[ind, 'freq_pct'] = '<' + str((100 * 5/n_total).round(1))          

      # ID      
      _summstat.loc[ind, 'todrop'] = 0
      #(col in _df_out['id_var'].unique().tolist()) |
      if(( (bool(re.search('^(PERSON_ID|EPIKEY)', col))) | (col in _idvarlist)) & (_summstat.loc[ind, 'val'] not in ['', None])): 
        _summstat.loc[ind, 'val'] = '<REMOVED>'         
        _summstat.loc[ind, 'todrop'] = 1
           
    _summstat = _summstat[_summstat['todrop'] != 1]
    _summstat = _summstat\
      .drop(['todrop'], axis=1)
    
    # add cols
    _summstat['name'] = _name
    _summstat['path'] = _path    
    _summstat['var'] = col
    
    # reorder cols
    cols = _summstat.columns.tolist()
    cols = cols[-3:] + cols[:-3]    
    _summstat = _summstat[cols] 
    
    if(i == 0): _summstat_m = _summstat
    else: _summstat_m = _summstat_m.append(_summstat)   
      
    _summstat_m = _summstat_m.reset_index(drop=True)
    
  return _summstat_m

# COMMAND ----------

def check_dis(df, var1, byvar='', _bins=50):
  # taken from age plot in skinny checks
    
#   tmp = df\
#     .select(var1)\
#     .toPandas()

  fig, axes = plt.subplots(1, 1, figsize=(13,4), sharex=True) # sharey=True , dpi=100) # 
  colors = sns.color_palette("tab10", 1)
#   names = ['gdppr', 'gdppr_snomed', 'hes_ae', 'hes_apc', 'hes_op']   

#   v = 'DOB'
#   tmp2d1 = tmp2 # dd2[dd2[f'_diff_{v}'] > -20]
#   s1 = list(tmp2d1[tmp2d1[f'_source_{v}'] == 'gdppr'][f'_age'])
#   s2 = list(tmp2d1[tmp2d1[f'_source_{v}'] == 'gdppr_snomed'][f'_age'])
#   s3 = list(tmp2d1[tmp2d1[f'_source_{v}'] == 'hes_ae'][f'_age'])
#   s4 = list(tmp2d1[tmp2d1[f'_source_{v}'] == 'hes_apc'][f'_age'])
#   s5 = list(tmp2d1[tmp2d1[f'_source_{v}'] == 'hes_op'][f'_age'])
#   axes.hist([s1, s2, s3, s4, s5], bins = list(np.linspace(0,120,100)), stacked=True, color=colors, label=names) # normed=True
#   axes.set_title(f'_age')
#   axes.legend(loc='upper left')

#   plt.tight_layout();
#   display(fig)
  # bins = list(np.linspace(0,120,100))
  #axes.hist([list(tmp[[f'{var1}']])], bins = 10, color=colors) #stacked=True, label=names) # normed=True
  axes.hist(df.select(var1).toPandas()[var1], bins = _bins, color=colors)
  # hist(axes, df.select(var1), bins = 50, color=['red'])
  
  return fig

# COMMAND ----------

# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")

# COMMAND ----------

# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

def temp_save(df, out_name:str, _dbc:str=f'dsa_391419_j3w9t_collab'):
  
  spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
  
  dfname = [x for x in globals() if globals()[x] is df]
  if(len(dfname) == 1): dfname = dfname[0]
  else: dfname = "unknown dfname (not in globals())"      
  
  print(f'saving {dfname} to {_dbc}.{out_name}')
  if(out_name != out_name.lower()):
    out_name_old = out_name
    out_name == out_name.lower()
    print(f'  out_name changed to lower case (from {out_name_old} to {out_name})')
    
  # save  
  df.write.mode('overwrite').saveAsTable(f'{_dbc}.{out_name}')
  # spark.sql(f'ALTER TABLE {_dbc}.{out_name} OWNER TO {_dbc}')
  print(f'  saved')
  
  # repoint
  spark.sql(f'REFRESH TABLE {_dbc}.{out_name}')
  return spark.table(f'{_dbc}.{out_name}')

# COMMAND ----------

import traceback
import warnings
from typing import Dict, List
from pyspark.sql import DataFrame as SparkDataFrame

def get_codematches(dataset_with_codelist: Dict[SparkDataFrame,SparkDataFrame], priority_for_ties: List[str], broadcast: bool) -> dict:
  """
  TODO
  Takes a dictionary and with keys as SparkDataFrame, values as the codelist used to join. 

  Returns two dictionaries dict_codelist and dict_codematch. 
  
  """
  for dataframe, codelist in dataset_with_codelist.items():
    # ------------------------------------------------------------------------------------
    # match the dataset and codelist
    # ------------------------------------------------------------------------------------
    print(f'Matching codes in the codelist to the datafame')
    try:
      if not type(broadcast) is bool:
         raise TypeError(f"Only Boolean values are allowed a type {type(broadcast)} was passed")
      if broadcast == True:
        codelist_matches_df = dataframe.join(f.broadcast(codelist), on='code', how='inner') #code is kind of hidden away here and uses the formates of the codelist tables, hopefully the eceception wil catch.
      else:
        codelist_matches_df = dataframe.join(codelist, on='code', how='inner') 
    except Exception: 
      print(traceback.format_exc())
   
    dict_codelist = {} # empty dict for codelist.
    dict_codematch = {} # empty dict for codematches.
      
    # add source and order to codelist_matches_df through priority_for_ties
    try:
      for dataset_priority in priority_for_ties:
        prioritised_codelist_matches_df = codelist_matches_df\
          .withColumn('source', f.lit(dataset_priority))\
          .withColumn('sourcen', f.lit(priority_for_ties.index(dataset_priority) + 1))
        
        # store the results in a dictionary with the codelist and the codelist joined the dataset. 
        dict_codelist[dataset_priority] = codelist
        dict_codematch[dataset_priority] = prioritised_codelist_matches_df
        
    except Exception:
      print(traceback.format_exc())

    # append
    print(f'\n append codelist matches from different sources/terminologies')
    try: 
      # codelist_all = reduce(DataFrame.unionByName, dict_codelist.values())  # This reduce runs on driver node so have refacfored, not yet certain if this is faster, but it will be more fault tolerant. 
      codelist_all = list(dict_codelist.items())[0][1] #First value is a DataFrame
      for df in list(dict_codelist.items())[0][1:]: #Iterate through the rest of the dataframes after the first one
        codelist_all = codelist_all.union(df)

      # codematch_all = reduce(DataFrame.unionByName,  dict_codematch.values())  # This reduce runs on driver node so have refacfored, but it will be more fault tolerant. 
      codematch_all = list(dict_codematch.items())[0][1] #First value is a DataFrame
      for df in list(dict_codematch.items())[0][1:]: #Iterate through the rest of the dataframes after the first one
        codematch_all = codematch_all.union(df)

      dict_codematch['combined_matched_dfs'] = codematch_all
      dict_codelist['combined_codelist_dfs'] = codelist_all
   
    except Exception:
          print(traceback.format_exc())
    
    return dict_codelist , dict_codematch
  
def get_codematches_event_order(dataset_with_codelist_dict: dict, name_of_dataset: str = 'combined_matched_dfs', last_event: bool = False):
  """
   TODO
  """
    
#     dict_codematch['combined_matched_dfs']
#     ------------------------------------------------------------------------------------
#     first/last event of each name
#     ------------------------------------------------------------------------------------
#   if(stages_to_run in [2,3]):
  codematch_all = dataset_with_codelist_dict[name_of_dataset]

  if last_event == True:
    print(f'filter to LAST event')
    win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy(f.desc('DATE'), 'sourcen', 'code')      
  else:
    print(f'filter to 1st event')
    win = Window\
      .partitionBy(['PERSON_ID', 'name'])\
      .orderBy('DATE', 'sourcen', 'code')  

  # filter
  codematch_1st = codematch_all\
    .withColumn('_rownum', f.row_number().over(win))\
    .where(f.col('_rownum') == 1)\
    .select('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END', 'DATE', 'name', 'source', 'code')\
    .orderBy('PERSON_ID', 'DATE', 'name')

#     # ------------------------------------------------------------------------------------
#     # identify ties by source for first event of each name
#     # ------------------------------------------------------------------------------------
  print(f'identify ties from sources/terminologies for first (/last) event')
  codematch_1st_tie_source = codematch_all\
    .withColumn('_tie', f.dense_rank().over(win))\
    .where(f.col('_tie') == 1)\
    .groupBy('PERSON_ID', 'name')\
    .agg(\
      f.countDistinct(f.col('source')).alias(f'_n_distinct_source')\
      , f.countDistinct(f.when(f.col('source').isNull(), 1)).alias(f'_null_source')\
      , f.sort_array(f.collect_set(f.col('source'))).alias('_tie_source_list')\
    )\
    .withColumn('_tie_source', f.when((f.col('_n_distinct_source') + f.col(f'_null_source')) > 1, 1).otherwise(0))\
    .select('PERSON_ID', 'name', '_tie_source', '_tie_source_list')

  codematch_1st = codematch_1st\
    .join(codematch_1st_tie_source, on=['PERSON_ID', 'name'], how='left')
      
  return  codematch_1st


def get_datset_reshaped_long_to_wide(_codematch_1st,
                                     name_prefix,
                                     codelist_dict: dict, 
                                     name_of_codelist: str = 'combined_codelist_dfs'
                                     
                                     ):
  """
   TODO
  Reshapes an ordered dataframe from wide to long.
  
  Args:
  
  Example Usage:
  
  Notes:
  
  
  """
  # ------------------------------------------------------------------------------------
  # reshape
  # ------------------------------------------------------------------------------------
  print(f'reshape long to wide')
  _codelist_all = codelist_dict[name_of_codelist]
  # join codelist names before reshape to ensure all covariates are created (when no code matches are found)
  _codelist_all = _codelist_all\
    .select('name')\
    .distinct() 

  # reshape long to wide  
  _codematch_1st_wide = _codematch_1st\
    .drop('code')\
    .join(_codelist_all, on='name', how='outer')\
    .withColumn('name', f.concat(f.lit(f'{name_prefix}'), f.lower(f.col('name'))))\
    .groupBy('PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END')\
    .pivot('name')\
    .agg(f.first('DATE'))\
    .where(f.col('PERSON_ID').isNotNull())\
    .orderBy('PERSON_ID')  
    
  # add flag and date columns
  print(f'add flag and date')
  vlist = []
  for i, v in enumerate([col for col in list(_codematch_1st_wide.columns) if re.match(f'^{name_prefix}', col)]):
    print(' ' , i, v)
    _codematch_1st_wide = _codematch_1st_wide\
      .withColumnRenamed(v, v + '_date')\
      .withColumn(v + '_flag', f.when(f.col(v + '_date').isNotNull(), 1))
    vlist = vlist + [v + '_flag', v + '_date']
  _codematch_1st_wide = _codematch_1st_wide\
    .select(['PERSON_ID', 'CENSOR_DATE_START', 'CENSOR_DATE_END'] + vlist)   
  
  return _codematch_1st_wide

# COMMAND ----------

# def snapshot(df, snapshot_name, snapshot_list_name, dbc='dars_nic_391419_j3w9t_collab'):
#   """
#   test
  
#   """
  
#   spark.conf.set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true')
  
#   snapshot_list_name = f'{snapshot_list_name}'.lower()
#   snapshot_name = f'{snapshot_name}'.lower()
    
#   # ------------------------------------------------------------------------------
#   # snapshot 
#   # ------------------------------------------------------------------------------     
#   df.write.mode('overwrite').saveAsTable(f'{dbc}.{snapshot_name}')
#   spark.sql(f'ALTER TABLE {dbc}.{snapshot_name} OWNER TO {dbc}')
#   spark.sql(f'REFRESH TABLE {dbc}.{snapshot_name}')
  
#   # ------------------------------------------------------------------------------
#   # snapshot list
#   # ------------------------------------------------------------------------------  
#   # check for snapshot list table
#   n1 = spark.sql(f"SHOW TABLES FROM {dbc}")\
#     .where(f.col('tableName') == snapshot_list_name)\
#     .count()
  
#   # create a df with snapshot_name and snapshot_datetime
#   datetimenow = datetime.datetime.now()
#   tmp1 = spark.createDataFrame([(snapshot_name, datetimenow)], ['snapshot_name', 'snapshot_datetime'])
#   if(n1 == 0): tmpM = tmp1
#   else:
#     # spark to pandas to spark to break lineage to allow overwriting a table we are reading from
#     tmp2 = spark.table(f'{dbc}.{snapshot_list_name}')\
#       .where(f.col('snapshot_name') != snapshot_name)\
#       .unionByName(tmp1)\
#       .toPandas()
#     tmpM = spark.createDataFrame(tmp2) 
#   tmpM.write.mode('overwrite').saveAsTable(f'{dbc}.{snapshot_list_name}')
#   spark.sql(f'ALTER TABLE {dbc}.{snapshot_list_name} OWNER TO {dbc}')  
#   spark.sql(f'REFRESH TABLE {dbc}.{snapshot_list_name}')
  
  
#   return spark.table(f'{dbc}.{snapshot_name}')

# COMMAND ----------

# for v in list(vars().keys()):
#   if(re.search("dataframe", type(vars()[v]).__name__.lower())):
#     print(v, type(vars()[v]))

# COMMAND ----------

def save_table(df, out_name:str, save_previous=True, data_base:str=f'dsa_391419_j3w9t_collab'):
  
  # assert that df is a dataframe
  assert isinstance(df, f.DataFrame), 'df must be of type dataframe' #isinstance(df, pd.DataFrame) | 
  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))
  
  # save name check
  if(any(char.isupper() for char in out_name)): 
    print(f'Warning: {out_name} converted to lowercase for saving')
    out_name = out_name.lower()
    print('out_name: ' + out_name)
    print('')
  
  # df name
  df_name = [x for x in globals() if globals()[x] is df][0]
  
  # ------------------------------------------------------------------------------------------------
  # save previous version for comparison purposes
  if(save_previous):
    tmpt = (
      spark.sql(f"""SHOW TABLES FROM {data_base}""")
      .select('tableName')
      .where(f.col('tableName') == out_name)
      .collect()
    )
    if(len(tmpt)>0):
      # save with production date appended
      _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
      out_name_pre = f'{out_name}_pre{_datetimenow}'.lower()
      print(f'saving (previous version):')
      print(f'  {out_name}')
      print(f'  as')
      print(f'  {out_name_pre}')
      spark.table(f'{data_base}.{out_name}').write.mode('overwrite').saveAsTable(f'{data_base}.{out_name_pre}')
      #spark.sql(f'ALTER TABLE {data_base}.{out_name_pre} OWNER TO {data_base}')
      print('saved')
      print('') 
    else:
      print(f'Warning: no previous version of {out_name} found')
      print('')
  # ------------------------------------------------------------------------------------------------  
  
  # save new version
  print(f'saving:')
  print(f'  {df_name}')
  print(f'  as')
  print(f'  {out_name}')
  df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(f'{data_base}.{out_name}')
  #spark.sql(f'ALTER TABLE {data_base}.{out_name} OWNER TO {data_base}')
  print('saved')

# COMMAND ----------

def table_summ(_data, _convarlist=[], _idvarlist=[], _sdc=False, _name=''):

  '''
  Description:
    Produce descriptive summary statistics for a table. 
    Unpack the following 3 summary statistics table outputs:
      1. continuous (numeric and date) variables
      2. categorical variables
      3. categorical variables value frequencies
  
  Arguments:
    _data: string or pyspark dataframe. Provide path (string) to a spark table or a pyspark dataframe name. You may wish to use the latter if preprocessing is required on some columns.
    _convarlist: list. Variables of string type that should be treated as numeric type and thus included in the contiuous summary.
    _idvarlist: list. Variables that should be treated as IDs and thus only be included in the first categorical summary; e.g. PERSON_ID_DEID.
    _sdc: logical. If summaries should apply statistical disclosure control. 
    _name: string. If included a new column will be added with the dataset name provided. This may be useful if merging data summary tables together for different datasets.
    
  Usage:
    acs_con, acs_cat1, acs_cat2 = table_summ(_data = f'{dbc}.nicor_acs_combined_{db}_archive', _idvarlist=["PERSON_ID_DEID","BatchId","HOSPITAL_IDENTIFIER"])
       
  Notes:
    - You need to provide 3 table names to the function as 3 sets of summary statistics are unpacked from the function. 
      In cases where no continuous or categorical varibales are present, you should still provide 3 table names to the function. A warning will be printed and the function can handle this.
    - This function treats any datatypes that are not int, float, smallint, date or timestamp as a string.
    - Timestamp variables are cast and handled as dates.
    - Note that this function does not handle categorical variables that are multivalue in nature and are stored in a nested form; i.e. in the same row. Appropriate preprocessing is required here.
    - No rounding is applied to summary statistics
  
  See Also:
    table_summ_test_sdc - applies sdc to summary statistics tables that have previously been produced using this function and in which had no sdc applied;
    i.e. where this function has been used with _sdc=False, table_summ_test_sdc will apply _sdc=True to the tables without the need to recompute the underlying summary statistics.
  
  Examples:
  
  
  Status:
    In testing stage 1 - see /Workspaces/dars_nic_391419_j3w9t_collab/SHDS/Fionna/Function Testing/table_summ_test
    
  Further Development:
    - The categorical variables value frequencies table produced is yet to have sdc formatting applied.
    - This function will not account for all variable types: the log will highlight any new data types that the function encourters. 
      New data types are treated as strings but it may be more appropriate to treat these as continuous variables, and in these cases the function should be updated to reflect this.
  
  Version control:
    Version 2
    This is version 2 of table_summ_2 written by Tom Bolton (Health Data Science Team, BHF Data Science Centre).
    
  Author: Fionna Chalmers

  
  '''
  
  # establish table
  if isinstance(_data, f.DataFrame):
    _tmp = _data
  elif isinstance(_data, str):
    _tmp = spark.table(_data)
  else:
    _tmp = print("Error: please provide a spark dataframe or a path to a spark dataframe to _data")
    exit()
    
  # initialise empty tables to ensure a table can be returned in cases where no con/cat variables present
  _summstat_con_m = pd.DataFrame()
  _summstat_cat1_m = pd.DataFrame()
  _summstat_cat2_m = pd.DataFrame()
  _summstat_con_ = pd.DataFrame()
  _summstat_cat1 = pd.DataFrame()
  _summstat_cat2 = pd.DataFrame()
  
  # dev - see note on line 81
  new_dtypes = []
  
  # loop over columns
  i1 = -1
  i2 = -1
  for i, col in enumerate(_tmp.columns):
    
    # establish data type of column
    dt = dict(_tmp.dtypes)[col]
    
    # select the column
    _tmp_col = (_tmp.select(col))
    
    # DEVELOPMENT ###############################################################################################################
    # for dev testing purposes: catch cases in which col types are not string | date | timestamp | float | int | smallint
    # currently data types that are not the aforementioned are treated as string and thus categoricals
    # a warning message will detail any new data types detected and in the case that these types are better treated as continuous,
    # the function should be updated appropriately
    # ###########################################################################################################################
    if(dt not in ['date','timestamp','float','int','smallint','string']):
      new_dtypes.append(dt)
        
    
    # =================================================================================
    # continous variables
    # =================================================================================
    varlist = ['mean', 'p025', 'p25', 'p50', 'p75', 'p975']
    
    if(((dt in ['date','timestamp','float','int','smallint']) | (col in _convarlist)) & (col not in _idvarlist)):
      
      i1 = i1 + 1
      
      # -------------------------------------------------------------------------------
      # dates
      # -------------------------------------------------------------------------------
      if((dt == 'date') | (dt == 'timestamp')): 
        print('  ' + str(i) + '/' + str(len(_tmp.columns) - 1) + ' ' + col + ' (' + dt + ') - CON (Date)', flush=True)
        
        # timestamps are cast to dates
        if((dt == 'timestamp')):
          _tmp_col = (
            _tmp_col
            .withColumn(col, f.to_date(f.col(col)))
          )
        else: _tmp_col = _tmp_col
        
        # convert date to number of days to reference date
        _tmp_col = (
          _tmp_col
          # convert date to number of days to reference date
          .withColumnRenamed(col, col + '_date')
          .withColumn(col + '_ndays', f.datediff(f.col(col + '_date'), f.to_date(f.lit("1960-01-01"), "yyyy-MM-dd")))
        )

        # summary statistics
        _summstat_con = (
          _tmp_col
          .agg(
            f.count(f.col(col + '_date')).alias('n')
            , f.sum((f.col(col + '_date').isNull()).cast('int')).alias('n_null')
            , f.countDistinct(f.col(col + '_date')).alias('n_distinct')
            , f.mean(f.col(col + '_ndays')).alias('mean')
            , f.stddev(f.col(col + '_ndays')).alias('stddev')
            , f.min(f.col(col + '_date')).alias('min')
            , f.expr('percentile(' + col + '_ndays' + ', 0.025)').alias('p025')
            , f.expr('percentile(' + col + '_ndays' + ', 0.25 )').alias('p25')
            , f.expr('percentile(' + col + '_ndays' + ', 0.5  )').alias('p50')
            , f.expr('percentile(' + col + '_ndays' + ', 0.75 )').alias('p75')
            , f.expr('percentile(' + col + '_ndays' + ', 0.975)').alias('p975')
            , f.max(f.col(col + '_date')).alias('max')
          )
          .toPandas()
        )
        
        # convert from ndays to date
        for var in varlist:
          _summstat_con[var] = (pd.to_datetime('1960-01-01') + pd.to_timedelta(_summstat_con[var], unit="D")).dt.date
        # tidy up
        if((pd.isna(_summstat_con['stddev'].min())) & (pd.isna(_summstat_con['stddev'].max()))):
          print('  stddev nan - values all null')
        else:
          _summstat_con['stddev'] = _summstat_con['stddev'].round(1)
          
      # -------------------------------------------------------------------------------
      # numeric
      # -------------------------------------------------------------------------------      
      elif((dt == 'float') | (dt == 'int') | (dt == 'smallint') | (col in _convarlist)): 
        print('  ' + str(i) + '/' + str(len(_tmp.columns) - 1) + ' ' + col + ' (' + dt + ') - CON', flush=True)

        # summary statistics
        _summstat_con = (
          _tmp_col
          .withColumn(col, f.round(f.col(col), 2).cast('double'))
          .agg(
            f.count(f.col(col)).alias('n')
            , f.sum((f.col(col).isNull()).cast('int')).alias('n_null')
            , f.countDistinct(f.col(col)).alias('n_distinct')
            , f.mean(f.col(col)).alias('mean')
            , f.stddev(f.col(col)).alias('stddev')
            , f.min(f.col(col)).alias('min')
            , f.expr('percentile(' + col + '' + ', 0.025)').alias('p025')
            , f.expr('percentile(' + col + '' + ', 0.25 )').alias('p25')
            , f.expr('percentile(' + col + '' + ', 0.5  )').alias('p50')
            , f.expr('percentile(' + col + '' + ', 0.75 )').alias('p75')
            , f.expr('percentile(' + col + '' + ', 0.975)').alias('p975')
            , f.max(f.col(col)).alias('max')
          )
          .toPandas()
        )
        
        # tidy up
        _summstat_con['min'] = _summstat_con['min'].round(1)
        _summstat_con['max'] = _summstat_con['max'].round(1)
        _summstat_con['stddev'] = _summstat_con['stddev'].round(1)
        _summstat_con[varlist] = _summstat_con[varlist].round(1)
        # -------------------------------------------------------------------------------

      # attach variable name and reorder
      _summstat_con['var'] = col
      cols = _summstat_con.columns.tolist()
      cols = cols[-1:] + cols[:-1]    
      _summstat_con = _summstat_con[cols]
      
      # return tables as str so that date and numeric columns can be binded
      if(i1 == 0): 
        _summstat_con_m = _summstat_con.astype(str)
      else: 
        _summstat_con_m = _summstat_con_m.append(_summstat_con).astype(str)
      
      
    # =================================================================================
    # categorical variables
    # =================================================================================
    else:
      if(col in _idvarlist): print('  ' + str(i) + '/' + str(len(_tmp.columns) - 1) + ' ' + col + ' (' + dt + ') - CAT (ID)', flush=True)
      else: print('  ' + str(i) + '/' + str(len(_tmp.columns) - 1) + ' ' + col + ' (' + dt + ') - CAT', flush=True) 
      
      i2 = i2 + 1
      
      # -------------------------------------------------------------------------------
      # summary statistics
      # -------------------------------------------------------------------------------
          
      _summstat_cat1 = (
        _tmp_col
        .agg(
          f.count(f.col(col)).alias('n')
          , f.sum((f.col(col).isNull()).cast('int')).alias('n_null')
          , f.countDistinct(f.col(col)).alias('n_distinct')
        )
        .toPandas()
        .astype(int)
      )

      n_distinct = _summstat_cat1['n_distinct'][0]
            
      # -------------------------------------------------------------------------------
      # frequencies
      # -------------------------------------------------------------------------------
      
      if(col not in _idvarlist): 
        
        _summstat_cat2 = (
          _tmp_col
          .groupBy(col)
          .count()
          .toPandas()
          .rename(columns={col: 'val', 'count': 'freq_n'})
        )
        
        n_total = _summstat_cat2['freq_n'].sum()
        
        # formatting
        _summstat_cat2['freq_pct'] = (100 * _summstat_cat2['freq_n'] / n_total).round(1)
        _summstat_cat2 = _summstat_cat2.astype({'freq_n': int})
        _summstat_cat2['val'] = _summstat_cat2['val'].fillna('<None>') 
        
        
        #if(n_distinct < 20): 
          #_summstat_cat2 = _summstat_cat2.sort_values('val')
          #_summstat_cat2['freq_note'] = ''
          
        #else: 
          #_summstat_cat2 = _summstat_cat2.sort_values('freq_n', ascending=False)
          #_summstat_cat2 = _summstat_cat2.astype({'freq_n': int})
          #_summstat_cat2['freq_note'] = '** top 10 most freq categ only **'
      
      # -------------------------------------------------------------------------------
      
      # attach variable name and reorder for summary statistics
      _summstat_cat1['var'] = col
      cols = _summstat_cat1.columns.tolist()
      cols = cols[-1:] + cols[:-1]    
      _summstat_cat1 = _summstat_cat1[cols]
      # attach variable name and reorder for frequencies
      if(col not in _idvarlist): 
        _summstat_cat2['var'] = col
        _summstat_cat2=_summstat_cat2.sort_values(by=['var','freq_n'], ascending=[False,False])
        cols = _summstat_cat2.columns.tolist()
        cols = cols[-1:] + cols[:-1]    
        _summstat_cat2 = _summstat_cat2[cols] 
      
      # return tables as strings for binding
      if(i2 == 0): 
        _summstat_cat1_m = _summstat_cat1.astype(str)
        _summstat_cat2_m = _summstat_cat2.astype(str)
      else: 
        _summstat_cat1_m = _summstat_cat1_m.append(_summstat_cat1).astype(str)
        if(col not in _idvarlist): 
          _summstat_cat2_m = _summstat_cat2_m.append(_summstat_cat2).astype(str)

  
  
  # =================================================================================
  # out of loop formatting
  # =================================================================================
  
  # store names of count columns
  if(_summstat_con_m.empty==False):
    con_format_cols = [col for col in [col for col in _summstat_con_m if col.startswith('n')] if 'distinct' not in col]
  if(_summstat_cat1_m.empty==False):
    cat1_format_cols = [col for col in [col for col in _summstat_cat1 if col.startswith('n')] if 'distinct' not in col]
  # convert to int for further formatting or sdc
  if(_summstat_con_m.empty==False):
    _summstat_con_m[con_format_cols+['n_distinct']] = _summstat_con_m[con_format_cols+['n_distinct']].astype(float).astype(int)
  if(_summstat_cat1_m.empty==False):
    _summstat_cat1_m[cat1_format_cols+['n_distinct']] = _summstat_cat1_m[cat1_format_cols+['n_distinct']].astype(float).astype(int)
  #_summstat_cat2[cat2_format_cols] = _summstat_cat2[cat2_format_cols].astype(float).astype(int)

  
  # =================================================================================
  # statistical disclosure control applied to all tables
  # =================================================================================
  if(_sdc):
    print('--------------------------------------------------------------------------------------', flush=True)
    print('SDC has been applied:', flush=True)
    print('- suppression of counts <10:', flush=True)
    print('- rounding of numbers >=10 to the nearest multiple of 5', flush=True)
    print('Note: n_distinct is number of distinct values that the given variable takes thus non-disclosive', flush=True)
    print('*** Remember to check across rows and cols in which no sdc has been applied ***')
    
    # -------------------------------------------------------------------------------
    # continuous summary stats
    # -------------------------------------------------------------------------------
    
    # ensure there are continuous variables
    if(_summstat_con_m.empty==False):
      # round numbers >=10 to the nearest multiple of 5
      _summstat_con_m[con_format_cols] = np.where(_summstat_con_m[con_format_cols]>= 10,
                                                  5*np.round(_summstat_con_m[con_format_cols]/5),
                                                  _summstat_con_m[con_format_cols])
      # remove disclosive descriptors
      ds_format_cols = ['min', 'max']
      _summstat_con_m[ds_format_cols] = '<removed>'
      # suppress counts <10
      _summstat_con_m[con_format_cols+['n_distinct']]=_summstat_con_m[con_format_cols+['n_distinct']].astype(float).astype(int).applymap("{:,}".format)
      _summstat_con_m[con_format_cols] = np.where(_summstat_con_m[con_format_cols].isin(list(map(str, range(0, 10)))),
                                                  '<10',
                                                  _summstat_con_m[con_format_cols])
      # update names of any cols in which sdc has been applied
      _summstat_con_m=_summstat_con_m.rename({col:col+'_sdc' for col in _summstat_con_m.columns[_summstat_con_m.columns.isin(con_format_cols+ds_format_cols)]}, axis=1)
    
    # -------------------------------------------------------------------------------
    # categorical summary stats
    # -------------------------------------------------------------------------------
    
    # ensure there are categorical variables
    if(_summstat_cat1_m.empty==False):
      # round numbers >=10 to the nearest multiple of 5
      _summstat_cat1_m[cat1_format_cols] = np.where(_summstat_cat1_m[cat1_format_cols]>= 10,
                                                    5*np.round(_summstat_cat1_m[cat1_format_cols]/5),
                                                    _summstat_cat1_m[cat1_format_cols])
      # suppress counts <10
      _summstat_cat1_m[cat1_format_cols+['n_distinct']]=_summstat_cat1_m[cat1_format_cols+['n_distinct']].astype(float).astype(int).applymap("{:,}".format)
      _summstat_cat1_m[cat1_format_cols] = np.where(_summstat_cat1_m[cat1_format_cols].isin(list(map(str, range(0, 10)))),
                                                    '<10',
                                                    _summstat_cat1_m[cat1_format_cols])
      # update names of any cols in which sdc has been applied
      _summstat_cat1_m=_summstat_cat1_m.rename({col:col+'_sdc' for col in _summstat_cat1_m.columns[_summstat_cat1_m.columns.isin(cat1_format_cols)]}, axis=1)
    
  else:
    print('--------------------------------------------------------------------------------------', flush=True)
    print('No SDC applied', flush=True)
    if(_summstat_con_m.empty==False):
      _summstat_con_m[con_format_cols+['n_distinct']]=_summstat_con_m[con_format_cols+['n_distinct']].astype(float).astype(int).applymap("{:,}".format)
    if(_summstat_cat1_m.empty==False):
      _summstat_cat1_m[cat1_format_cols+['n_distinct']]=_summstat_cat1_m[cat1_format_cols+['n_distinct']].astype(float).astype(int).applymap("{:,}".format)

  

  # =================================================================================
  # final formatting steps
  # =================================================================================

  if(_summstat_con_m.empty & _summstat_cat1_m.empty):
    print('--------------------------------------------------------------------------------------', flush=True)
    print('No continous variables to summarise', flush=True)  
    print('No categorical variables to summarise', flush=True)  
  elif(_summstat_con_m.empty):
    print('--------------------------------------------------------------------------------------', flush=True)
    print('No continous variables to summarise', flush=True)
  elif(_summstat_cat1_m.empty):
    print('--------------------------------------------------------------------------------------', flush=True)
    print('No categorical variables to summarise', flush=True)
  
  # dataset name
  if((_name!='') & (isinstance(_name,str))):
    _summstat_con_m.insert(loc = 0,column = 'name',value = [_name]*_summstat_con_m.shape[0])
    _summstat_cat1_m.insert(loc = 0,column = 'name',value = [_name]*_summstat_cat1_m.shape[0])
    _summstat_cat2_m.insert(loc = 0,column = 'name',value = [_name]*_summstat_cat2_m.shape[0])
  elif((_name!='') & (isinstance(_name,str)==False)):
    if(_summstat_con_m.empty==False & _summstat_cat1_m.empty==False):
      print('--------------------------------------------------------------------------------------', flush=True)
      print('Warning: _name provided must be of type string. No name column added.', flush=True)
    else:
      print('Warning: _name provided must be of type string. No name column added.', flush=True)
    
  
  # drop index for all returned tables
  _summstat_con_m = _summstat_con_m.reset_index(drop=True)
  _summstat_cat1_m = _summstat_cat1_m.reset_index(drop=True)
  _summstat_cat2_m = _summstat_cat2_m.reset_index(drop=True)
  
  
  # developement check
  if(len(new_dtypes)>0):
    print('--------------------------------------------------------------------------------------', flush=True)
    print('New data types detected, please check: ' + str(list(set(new_dtypes))), flush=True)
    
  return _summstat_con_m, _summstat_cat1_m, _summstat_cat2_m

# COMMAND ----------

def get_provisioning_date(year=None, month=None) -> None:
    """
    Return the date at which data was provisioned for a given year and month. If year and month are not provided the most recent archived_on date is returned.

    Args:
        year (int): The year to filter for.
        month (int): The month to filter for (1-12).

    Returns:
        str: The provisioning date in 'YYYYMMdd' format

    Examples:
    >>> get_provisioning_date(year=2020,month=12)
            "More than one data provisioning batch found for this year and month. The most recent has been returned."
            "20201211"

    """
    import pyspark.sql.functions as f
    import datetime

    if year is None and month is None:
        provisioning_date = (
        (spark.table(f'dars_nic_391419_j3w9t_collab.wrang002b_data_version_batchids'))
        .orderBy(f.col("ProductionDate").desc())
        .select("ProductionDate")
        .limit(1)
        .withColumn("ProductionDate",f.to_date(f.col("ProductionDate")))
        .collect()[0][0]
        )

    if year is None and month is not None:
        raise ValueError("Please supply the year for filtering.")

    if month is None and year is not None:
        raise ValueError("Please supply the month for filtering.")

    if year is not None and month is not None:

        if month < 1 or month > 12:
            raise ValueError("Month must be within the range of 1-12.")

        else:
            provisioning_date_df = (
                (spark.table(f'dars_nic_391419_j3w9t_collab.wrang002b_data_version_batchids'))
                .select("ProductionDate")
                .filter(f.year('ProductionDate') == year)
                .filter(f.month('ProductionDate') == month)
                .withColumn("ProductionDate",f.to_date(f.col("ProductionDate")))
                .orderBy(f.col("ProductionDate").desc())
                )
            
            provisioning_date_df_length = provisioning_date_df.count()

            provisioning_date = (
                provisioning_date_df
                .collect()[0][0]
            )

            if(provisioning_date_df_length>1):
                print(f'More than one data provisioning batch found for this year and month. The most recent has been returned.')
      
    
    provisioning_date = provisioning_date.strftime('%Y%m%d')


    return provisioning_date

# COMMAND ----------

def union_dataframe_list( target_list ):
    assert type(target_list) is list , 'object is not a list'
    import pandas as pd

    for item in target_list:
        if item == target_list[0]:
            output = target_list[0].toPandas()
        else:
            output = pd.concat([output, item.toPandas()])

    #output = createDataFrame(output)
    return output    
