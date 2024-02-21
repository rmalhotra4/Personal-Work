!gcloud config set project geotab-serverops #project changed for lower traffic - less rate limiting

isAuto = False  #only use if the stage one batch is 10 servers or less (rate limiting)

if isAuto != False:
  majorVersion = "2104"  ##set desired major version for health check
else:
  serverList = ['GCEMYGEOTP468','GCEMYGEOTP502','GCEMYGEOTP567','GCEMYGEOTP73','GCEMYGEOTP760','GCEMYGEOTP825','GCEMYGEOTP850'] #set the desired machine names here (GCEMYG...)
  
  
from google.colab import auth
auth.authenticate_user()
!pip install --upgrade google-auth-oauthlib
from oauth2client.client import GoogleCredentials

import os
import time
import json
import random
import datetime as dt
import re
import pandas as pd

!pip install datalab
import datalab.bigquery as bq

import numpy as np
from threading import Thread

!pip install --upgrade plotly #always install newest version
import plotly
from plotly.subplots import make_subplots
import plotly.offline as offline
import plotly.graph_objs as go

plotly.offline.init_notebook_mode(connected=False)

def GetCPUAsync(MachineName, value = 14, datePart = 'DAY'):
  queryText = """
--This query pulls the CPU graph (including cores on the machine for you): 
 with CPU AS(
  SELECT
    Processor.CollectedDateTimeUTC,
    Processor.MachineName,
    (SUM(Checkmate_ProcessorTime_Windows)/(NumberLogicalCores)+sum(Checkmate_ProcessorTime_Linux)) AS Checkmate_ProcessorTime,
    sum(Postgres_ProcessorTime) as PostgresLinux_ProcessorTime,
    sum(Postgres_maintenance_ProcessorTime_Linux) as Postgres_maintenanceLinux_ProcessorTime,
    SUM(NodeJs_ProcessorTime)/(NumberLogicalCores) AS NodeJs_ProcessorTime,
    SUM(Total_ProcessorTime) AS Total_ProcessorTime
  FROM (
    SELECT
      CollectedDateTimeUTC,
      MachineName,
      (CASE
          WHEN (Counter = 'Process_CheckmateServer____Processor_Time') THEN Value
          ELSE 0 END) AS Checkmate_ProcessorTime_Windows,
      (CASE
          WHEN (Counter = 'Process__Total____Processor_Time_mygeotab') THEN Value
          ELSE 0 END) AS Checkmate_ProcessorTime_Linux,
      (CASE
          WHEN (Counter = 'Process__Total____Processor_Time_postgres_maintenance') THEN Value
          ELSE 0 END) AS Postgres_maintenance_ProcessorTime_Linux,
      (CASE
          WHEN (Counter = 'Process__node____Processor_Time') THEN Value
          ELSE 0 END) AS NodeJs_ProcessorTime,
      (CASE
          WHEN (Counter = 'Processor__Total____Processor_Time') THEN Value
          ELSE 0 END) AS Total_ProcessorTime,
        (CASE
            When (Counter= 'Process__Total____Processor_Time_postgres') then value else 0 end) as Postgres_ProcessorTime
    FROM
      `geotab-myserver.DataAgent.Performance_*`--Enter relevant timestamps here
    WHERE
      _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
    --   AND (Counter = 'Process_CheckmateServer____Processor_Time' or counter = 'Process__dotnet____Processor_Time'
    --     OR Counter = 'Processor__Total____Processor_Time')
      AND LOWER(MachineName) = lower('{0}')
      ) AS Processor --Enter MachineName here
  LEFT JOIN (
    SELECT
      NumberLogicalCores,
      MachineName
    FROM (
      SELECT
        CollectedDateTimeUTC,
        cast (NumberLogicalCores as float64) as NumberLogicalCores,
        MachineName,
        ROW_NUMBER() OVER (PARTITION BY NumberLogicalCores ORDER BY CollectedDateTimeUTC DESC) AS RN1
      FROM
        `geotab-myserver.DataAgent.InstalledApplications_*`
      WHERE
        _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
        AND LOWER(MachineName) = lower('{0}')) --Enter MachineName here 
    WHERE
      RN1 = 1) AS Cores
  ON
    Processor.MachineName = Cores.MachineName
  GROUP BY
    Processor.CollectedDateTimeUTC,
    Processor.MachineName,
    Cores.NumberLogicalCores
  ORDER BY
    Processor.CollectedDateTimeUTC DESC 
  ), period AS (
    SELECT MIN(CollectedDateTimeUTC) min_time, MAX(CollectedDateTimeUTC) max_time, TIMESTAMP_DIFF(MAX(CollectedDateTimeUTC), MIN(CollectedDateTimeUTC), MINUTE) diff
    FROM CPU
  ), checkpoints AS (
    SELECT TIMESTAMP_ADD(min_time, INTERVAL step MINUTE) start_time, TIMESTAMP_ADD(min_time, INTERVAL step + 5 MINUTE) end_time
    FROM period, UNNEST(GENERATE_ARRAY(0, diff + 5, 5)) step
  )
  SELECT start_time AS Processor_CollectedDateTimeUTC, 
  AVG(Checkmate_ProcessorTime) AS Checkmate_ProcessorTime, 
  --   AVG(CheckmateLinux_ProcessorTime) AS CheckmateLinux_ProcessorTime,
  avg(Postgres_maintenanceLinux_ProcessorTime) as Postgres_maintenanceLinux_ProcessorTime,
  AVG(NodeJs_ProcessorTime) AS NodeJs_ProcessorTime,
  AVG(Total_ProcessorTime) AS Total_ProcessorTime, 
  AVG(PostgresLinux_ProcessorTime) as PostgresLinux_ProcessorTime FROM checkpoints
  JOIN CPU ON CPU.CollectedDateTimeUTC >= checkpoints.start_time AND CPU.CollectedDateTimeUTC < checkpoints.end_time
  GROUP BY start_time
  ORDER BY start_time DESC
  """

  completeQueryText = queryText.format(MachineName, str(value), datePart)
  cpu_usage_query = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  cpu_usage_query_job = cpu_usage_query.execute_async(dialect = 'standard')

  return cpu_usage_query_job


def GetServerUpgrades(machineName, value = 14, datePart = 'DAY'):
  queryText = """
  SELECT CheckmateVersion AS Version, MIN(StatusDateUTC) AS DateTime
  FROM `geotab-myserver.ServerMonitors.MyServerInfo_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND ServerName like '{0}'
  GROUP BY Version
  ORDER BY Datetime DESC
  """

  servernumber = (machineName[10:])

  if ("GCEMYGEOTP" in machineName):
    servername = "my"+servernumber+".geotab.com"
  elif ("GCEMYGATT" in machineName):
    servername = "afmfe"+servernumber+".att.com"
  elif ("GOV" in machineName):
    servername = "gov"+servernumber+".geotab.com"
  elif ("GCEMYGMIRT" in machineName):
    servername = "mirror"+servernumber+".geotab.com"
  elif ("GCEMYGEOTT" in machineName):
    servername = "support"+servernumber+".geotab.com"
  elif ("GCEMYGEOTB" in machineName):
    servername = "preview"+servernumber+".geotab.com"
  else:
    pass

    
  completeQueryText = queryText.format(servername, str(value), datePart)
  ServerUpgradeQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  ServerUpgradeQueryJob = ServerUpgradeQuery.execute_async(dialect='standard')

  return ServerUpgradeQueryJob


def getMemoryAsync(machineName, value = 14, datePart = 'DAY'):
  queryText = """
   SELECT 
  CollectedDateTimeUTC , 
  SUM(Memory___Available_MBytes) as Available_Memory_GB, 
  SUM(Process_CheckmateServer__Working_Set) as Checkmate_Memory_GB,
  sum(Postgres__Working_Set) as Postgres_Memory_GB,
  SUM(Private_Bytes) AS Allocated_GB 
    FROM(
    SELECT CollectedDateTimeUTC, 
      CASE WHEN Counter = 'Memory___Available_MBytes' THEN Value/1024 ELSE 0 END AS Memory___Available_MBytes,
      CASE WHEN Counter = 'Process_CheckmateServer__Working_Set' THEN Value/(1024*1024*1024) 
      when counter = 'Memory___Used_MBytes_mygeotab' then value/1024 ELSE 0 END AS Process_CheckmateServer__Working_Set,
      CASE WHEN Counter =  'Process_CheckmateServer__Private_Bytes' THEN Value/(1024*1024*1024) 
      when counter = 'Memory___Available_MBytes_mygeotab' then value/1024 ELSE 0 END AS Private_Bytes,
      case when counter = 'Memory___Used_MBytes_postgres' then Value/(1024) ELSE 0 END AS Postgres__Working_Set
    From `geotab-myserver.DataAgent.Performance_*`  
    WHERE 
    _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
    AND LOWER(MachineName) = lower('{0}')
    --AND CollectedDateTimeUTC > TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 4 HOUR) 
    ) --Enter machine name here  
    GROUP BY CollectedDateTimeUTC 
    ORDER BY CollectedDateTimeUTC Desc
  """

  completeQueryText = queryText.format(machineName, str(value), datePart)
  memoryQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  MemoryQueryJob = memoryQuery.execute_async(dialect='standard')

  return MemoryQueryJob


def getPerformanceSecondsAsync(machineName, value = 14, datePart = 'DAY'):
  queryText = """
  with naming as (
    SELECT 
    distinct machinename,
    case when machinename like 'my%_mygeotab' then concat('GCEMYGEOTP',REGEXP_REPLACE(machinename, r'[^\d]+', ''))
    else machinename end MachineNameEntry
    FROM
        `geotab-myserver.MyGeotab.PerformanceTime_*`--Enter relevant timestamps here
    WHERE
      _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",CURRENT_DATE)
  )
  SELECT
   DateTime,
   IF(SUM(CpuSeconds) < 0, 0, SUM(CpuSeconds)) AS CpuSeconds,
   IF(SUM(Seconds) < 0, 0, SUM(Seconds)) AS ActualSeconds,
   Source
  FROM `geotab-myserver.MyGeotab.PerformanceTime_*`
  WHERE 
  _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND LOWER(MachineName) in (select lower(machinename) from naming where lower(MachineNameEntry) like LOWER("{0}"))
  AND Level1Action IS NOT NULL
  AND Level1Action != ""
  GROUP BY DateTime, Source
  ORDER BY DateTime
  """

  completeQueryText = queryText.format(machineName, str(value), datePart)
  PerformanceSecQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  PerformanceSecQueryJob = PerformanceSecQuery.execute_async(dialect='standard')

  return PerformanceSecQueryJob


def GetAPIUsageAsync(machineName, value = 14, datePart= 'DAY'):
  queryText = """
  with naming as (
    SELECT 
    distinct machinename,
    case when machinename like 'my%_mygeotab' then concat('GCEMYGEOTP',REGEXP_REPLACE(machinename, r'[^\d]+', ''))
    else machinename end MachineNameEntry
    FROM
        `geotab-myserver.MyGeotab.PerformanceTime_*`--Enter relevant timestamps here
    WHERE
      _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",CURRENT_DATE)
)
  SELECT timestamp_trunc(DateTime, minute) AS Minute10, CallName, SUM(SizeinKb) AS TotalSize, SUM(Seconds) AS TotalSecs, COUNT(*) AS Calls
    FROM (SELECT DateTime,CallName, SizeinKb, Seconds,MachineName FROM `geotab-myserver.MyGeotab.ApiUsage_*`
    WHERE 
    _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
    )
    WHERE CallName IN(SELECT CallName FROM(
    SELECT CallName, SUM(SizeinKb) AS TotalSize, SUM(Seconds) AS TotalSecs, COUNT(*) AS Calls
    FROM (SELECT * FROM `geotab-myserver.MyGeotab.ApiUsage_*`
    WHERE 
    _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
    )
    WHERE CallName <> 'Authenticate' AND CallName <> 'DatabaseExists'
    and LOWER(MachineName) in (select distinct lower(MachineName) from naming where lower(MachineNameEntry) like LOWER('{0}'))
    and CallName not like 'GetVersion'
    and CallName not like 'Subscribe'
    and CallName not like 'GetServerInfo'
    and CallName not like 'GetSystemTimeUTC'
    and CallName not like 'Unsubscribe'
    and CallName not like 'TraceText'
    and CallName not like 'GetXMLServerSettings'
    and CallName not like 'AuthenticateSaml'
    and CallName not like 'GetSystemTimeUtc'
    and CallName not like 'Get<SystemSettings>'
    GROUP BY CallName
    order by Calls desc
    LIMIT 8))
    AND LOWER(MachineName) in (select distinct lower(MachineName) from naming where lower(MachineNameEntry) like LOWER('{0}'))
    GROUP BY Minute10,CallName
    ORDER BY Minute10 desc
  """

  completeQueryText = queryText.format(machineName, str(value), datePart)
  APIUsageQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  APIUsageQueryJob = APIUsageQuery.execute_async(dialect='standard')

  return APIUsageQueryJob


def getPGStatusAsync(machineName, value = 14, datePart = 'DAY'):
  queryText = """
  with Queries AS (
  SELECT timestamp_trunc(CollectedDateTimeUTC,hour) as QueryDate, SUBSTR(LOWER(Query), 0, 200) AS ShortQuery,SUM(TotalTimeMilliseconds) AS TotTime_ms,SUM(Calls) AS TotCalls 
  FROM `geotab-myserver.DataAgent.PgStatQuery_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND UserName LIKE 'gu_%'
  AND LOWER(Query) NOT LIKE '%commit%'
  AND LOWER(Query) NOT LIKE '%begin%'
  AND LOWER(Query) NOT LIKE '%discard%'
  AND LOWER(Query) NOT LIKE '%vacuum%'
  AND LOWER(Query) NOT LIKE '%pg_database_size%'
  AND LOWER(Query) NOT LIKE '%pg_stat%'
  AND LOWER(Query) NOT LIKE '%processlock%'
  AND LOWER(MachineName) = LOWER("{0}")
  GROUP BY SUBSTR(LOWER(Query), 0, 200), QueryDate
  ), TopQueries AS (
  SELECT ShortQuery FROM (
  SELECT *, ((TotTime_ms/1000)/TotCalls) AS SecPerCall FROM Queries
  WHERE TotCalls > 1
  ORDER BY TotTime_ms DESC
  LIMIT 100 )
  ), MoreQueries AS (
  SELECT timestamp_trunc(CollectedDateTimeUTC,hour) as QueryDate, SUBSTR(LOWER(Query), 0, 200) AS ShortQuery,SUM(TotalTimeMilliseconds) AS TotTime_ms,SUM(Calls) AS TotCalls 
  FROM `geotab-myserver.DataAgent.PgStatQuery_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND UserName LIKE 'gu_%'
  AND LOWER(Query) NOT LIKE '%commit%'
  AND LOWER(Query) NOT LIKE '%begin%'
  AND LOWER(Query) NOT LIKE '%discard%'
  AND LOWER(Query) NOT LIKE '%vacuum%'
  AND LOWER(Query) NOT LIKE '%pg_database_size%'
  AND LOWER(Query) NOT LIKE '%pg_stat%'
  AND LOWER(Query) NOT LIKE '%processlock%'
  AND LOWER(MachineName) = LOWER("{0}")
  GROUP BY SUBSTR(LOWER(Query), 0, 200), QueryDate
  )

  SELECT MQ.QueryDate,SUBSTR(LOWER(MQ.ShortQuery), 0, 50) AS ShortQuery,MQ.TotTime_ms,MQ.TotCalls, ((TotTime_ms/1000)/TotCalls) AS SecPerCall FROM MoreQueries AS MQ
  RIGHT JOIN TopQueries AS TQ
  ON LOWER(MQ.ShortQuery) = LOWER(TQ.ShortQuery)
  ORDER BY ShortQuery DESC, QueryDate DESC
  """

  completeQueryText = queryText.format(machineName, str(value), datePart)
  PGStatQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  PGStatQueryJob = PGStatQuery.execute_async(dialect='standard')

  return PGStatQueryJob

def GetBadFileCountAsync(machineName, value = 14, datePart = 'DAY'):
  queryText = """
  WITH Status AS (
  SELECT ServerName, Name, BadFileCount, StatusDateUTC 
  FROM `geotab-myserver.ServerMonitors.MyServerDatabaseInfo_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND ServerName LIKE '{0}'
  )

  SELECT StatusDateUTC, ServerName, SUM(BadFileCount) AS TotalBadFiles FROM Status
  GROUP BY StatusDateUTC, ServerName
  ORDER BY StatusDateUTC DESC
  """

  servernumber = (machineName[10:])

  if ("GCEMYGEOTP" in machineName):
    servername = "my"+servernumber+".geotab.com"
  elif ("GCEMYGATT" in machineName):
    servername = "afmfe"+servernumber+".att.com"
  elif ("GOV" in machineName):
    servername = "gov"+servernumber+".geotab.com"
  elif ("GCEMYGMIRT" in machineName):
    servername = "mirror"+servernumber+".geotab.com"
  elif ("GCEMYGEOTT" in machineName):
    servername = "support"+servernumber+".geotab.com"
  elif ("GCEMYGEOTB" in machineName):
    servername = "preview"+servernumber+".geotab.com"
  else:
    pass


  completeQueryText = queryText.format(servername, str(value), datePart)
  BadFileCountQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  BadFileCountQueryJob = BadFileCountQuery.execute_async(dialect='standard')

  return BadFileCountQueryJob

def getBiggestDbAsync(machineName):
  queryText = """
  SELECT LOWER(Name) AS Name FROM (
  SELECT Name,DeviceCount,ServerName,StatusDateUTC, RANK() OVER(PARTITION BY Name,Servername ORDER BY StatusDateUTC DESC) AS Rn   FROM `geotab-myserver.ServerMonitors.MyServerDatabaseInfo_*`
  WHERE _TABLE_SUFFIX = FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND ServerName = '{0}'
  ) WHERE Rn = 1
  ORDER BY DeviceCount DESC
  LIMIT 1
  """
  servernumber = (machineName[10:])

  if ("GCEMYGEOTP" in machineName):
    servername = "my"+servernumber+".geotab.com"
  elif ("GCEMYGATT" in machineName):
    servername = "afmfe"+servernumber+".att.com"
  elif ("GOV" in machineName):
    servername = "gov"+servernumber+".geotab.com"
  elif ("GCEMYGMIRT" in machineName):
    servername = "mirror"+servernumber+".geotab.com"
  elif ("GCEMYGEOTT" in machineName):
    servername = "support"+servernumber+".geotab.com"
  elif ("GCEMYGEOTB" in machineName):
    servername = "preview"+servernumber+".geotab.com"
  else:
    pass


  completeQueryText = queryText.format(servername)
  BiggestDatabaseQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  BiggestDatabaseQueryJob = BiggestDatabaseQuery.execute_async(dialect='standard')

  return BiggestDatabaseQueryJob

def getLagAsync(databaseName, value = 14, datePart = 'DAY'):
  queryText = """
  SELECT DateTime,
  CASE WHEN Action = 'UploaderLagTime' THEN 'Processing Time' ELSE Replace(Action, 'DDIS.', '') END AS Action,
  AverageValue
  FROM `geotab-myserver.MyGeotab.PerformanceValue_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {1} {2})) AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_TIMESTAMP())
  AND LOWER(DATABASE) = '{0}'
  AND
  (Action IN ('UploaderLagTime','UploaderFileLengthKB','UploaderFileCount')OR Action Like 'DDIS.LagTime%')
  """

  completeQueryText = queryText.format(databaseName, str(value), datePart)
  LagQuery = bq.Query(completeQueryText)
  time.sleep(random.randint(0,20))
  LagQueryJob = LagQuery.execute_async(dialect='standard')

  return LagQueryJob


def getAsyncPlots(MachineName):
  ServerUpgrade = GetServerUpgrades(machineName = MachineName, value = 14, datePart = 'DAY')
  CpuUsage = GetCPUAsync(MachineName = MachineName, value = 14, datePart = 'DAY')
  MemoryUsage = getMemoryAsync(machineName = MachineName, value = 14, datePart = 'DAY')
  PerformanceSec = getPerformanceSecondsAsync(machineName = MachineName, value = 7, datePart = 'DAY')
  APIUsage = GetAPIUsageAsync(machineName = MachineName, value = 14, datePart = 'DAY')
  PgStatQuery = getPGStatusAsync(machineName = MachineName, value = 14, datePart = 'DAY')
  BadFileCountQuery = GetBadFileCountAsync(machineName = MachineName, value = 14, datePart = 'DAY')
  BiggestDB = getBiggestDbAsync(machineName = MachineName)
  df_BiggestDB = BiggestDB.results.to_dataframe()
  if (df_BiggestDB.empty != True):
    BiggestDB = df_BiggestDB['Name'][0]
  else:
    BiggestDB = ''

  LagTime = getLagAsync(databaseName = BiggestDB, value = 14, datePart = 'DAY')

  fig = plotly.subplots.make_subplots(rows=14, cols=1, subplot_titles=('CPU Plot', 'Memory Plot', 'CPU Seconds Plot', 'Actual Seconds Plot', 'API Call Volume Plot', 'API Call Size Plot', 'API Call Time Plot','Query Volume Plot', 'Sec Per Query Plot', 'Total Query Time Plot', 'Bad File Count', 'Lag Time Plot - {}'.format(BiggestDB)))

  dfServerUpgrade = ServerUpgrade.results.to_dataframe()
  if (dfServerUpgrade.empty != True):
    LastUpgrade = dfServerUpgrade['DateTime'][0]
    Version = dfServerUpgrade['Version'][0]
  else:
    LastUpgrade = ''
    Version = 'X.X.XXXX.XXX'

  cpuPerfDF = CpuUsage.results.to_dataframe()
  memoryPerfDF = MemoryUsage.results.to_dataframe()
  PerformanceSecDF = PerformanceSec.results.to_dataframe()
  APIUsageDF = APIUsage.results.to_dataframe()
  PgStatQueryDF = PgStatQuery.results.to_dataframe()
  BadFileCountDF = BadFileCountQuery.results.to_dataframe()
  LagTimeDF = LagTime.results.to_dataframe()

  CpuUsageTraces = []
  memoryUsageTraces = []
  BadFileCountTraces = []
  
  if (cpuPerfDF.empty != True):
    CpuUsageTraces.append(go.Scatter(x = cpuPerfDF['Processor_CollectedDateTimeUTC'], y = cpuPerfDF['Total_ProcessorTime'], name='TotalCPUMean',legendgroup='cpu'))
    CpuUsageTraces.append(go.Scatter(x = cpuPerfDF['Processor_CollectedDateTimeUTC'], y = cpuPerfDF['Checkmate_ProcessorTime'], name = 'CheckmateCPUMean',legendgroup='cpu'))
    CpuUsageTraces.append(go.Scatter(x = cpuPerfDF['Processor_CollectedDateTimeUTC'], y = cpuPerfDF['PostgresLinux_ProcessorTime'], name = 'PostgresLinuxCPUMean',legendgroup='cpu'))
    CpuUsageTraces.append(go.Scatter(x = cpuPerfDF['Processor_CollectedDateTimeUTC'], y = cpuPerfDF['NodeJs_ProcessorTime'], name = 'NodeJsCPUMean',legendgroup='cpu'))
    CpuUsageTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,100],text=Version,mode='text',legendgroup='cpu'))
    CpuUsageTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,100],name=Version,mode='lines',legendgroup='cpu'))


  if (memoryPerfDF.empty != True):
    memoryUsageTraces.append(go.Scatter(x = memoryPerfDF['CollectedDateTimeUTC'], y = memoryPerfDF['Available_Memory_GB'], name='TotalMemoryMean',legendgroup='memory'))
    memoryUsageTraces.append(go.Scatter(x = memoryPerfDF['CollectedDateTimeUTC'], y = memoryPerfDF['Checkmate_Memory_GB'], name='CheckmateMemoryMean',legendgroup='memory'))
    memoryUsageTraces.append(go.Scatter(x = memoryPerfDF['CollectedDateTimeUTC'], y = memoryPerfDF['Postgres_Memory_GB'], name='PostgresMemoryMean',legendgroup='memory'))
    memoryUsageTraces.append(go.Scatter(x = memoryPerfDF['CollectedDateTimeUTC'], y = memoryPerfDF['Allocated_GB'], name='AllocatedMemoryMean',legendgroup='memory'))
    memoryUsageTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,max(memoryPerfDF['Allocated_GB'])],text=Version,mode='text',legendgroup='memory'))
    memoryUsageTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,max(memoryPerfDF['Allocated_GB'])],name=Version,mode='lines',legendgroup='memory'))

  if (PerformanceSecDF.empty != True):
    df_CpuPerfReport_pivot = pd.pivot_table(PerformanceSecDF, values='CpuSeconds',index=['DateTime'],columns=['Source'], aggfunc=np.sum)
    CpuPerfReportTraces = [go.Scatter(x=df_CpuPerfReport_pivot.index,y=df_CpuPerfReport_pivot[col],name=col,legendgroup='cpuperfreport') for col in df_CpuPerfReport_pivot.columns]
    CpuPerfReportTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_CpuPerfReport_pivot)),max(pd.DataFrame.max(df_CpuPerfReport_pivot))],text=Version,mode='text',legendgroup='cpuperfreport'))
    CpuPerfReportTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_CpuPerfReport_pivot)),max(pd.DataFrame.max(df_CpuPerfReport_pivot))],name=Version,mode='lines',legendgroup='cpuperfreport'))

    df_ActualPerfReport_pivot = pd.pivot_table(PerformanceSecDF, values='ActualSeconds',index=['DateTime'],columns=['Source'], aggfunc=np.sum)
    ActualPerfReportTraces = [go.Scatter(x=df_ActualPerfReport_pivot.index,y=df_ActualPerfReport_pivot[col],name=col,legendgroup='actualperfreport') for col in df_ActualPerfReport_pivot.columns]
    ActualPerfReportTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_ActualPerfReport_pivot)),max(pd.DataFrame.max(df_ActualPerfReport_pivot))],text=Version,mode='text',legendgroup='actualperfreport'))
    ActualPerfReportTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_ActualPerfReport_pivot)),max(pd.DataFrame.max(df_ActualPerfReport_pivot))],name=Version,mode='lines',legendgroup='actualperfreport'))
  else:
    CpuPerfReportTraces = []
    ActualPerfReportTraces = []

  if (APIUsageDF.empty != True):
    df_APIUsage_pivot = pd.pivot_table(APIUsageDF, values = 'Calls',index=['Minute10'],columns=['CallName'],aggfunc=np.sum)
    APICallVolumeTraces = [go.Scatter(x=df_APIUsage_pivot.index,y=df_APIUsage_pivot[col],name=col,legendgroup='apivolume') for col in df_APIUsage_pivot.columns]
    APICallVolumeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APIUsage_pivot)),max(pd.DataFrame.max(df_APIUsage_pivot))],text=Version,mode='text',legendgroup='apivolume'))
    APICallVolumeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APIUsage_pivot)),max(pd.DataFrame.max(df_APIUsage_pivot))],name=Version,mode='lines',legendgroup='apivolume'))

    df_APICallSize_pivot = pd.pivot_table(APIUsageDF, values = 'TotalSize',index='Minute10',columns=['CallName'],aggfunc=np.sum)
    APICallSizeTraces = [go.Scatter(x=df_APICallSize_pivot.index,y=df_APICallSize_pivot[col],name=col,legendgroup='apicallsize') for col in df_APICallSize_pivot.columns]
    APICallSizeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APICallSize_pivot)),max(pd.DataFrame.max(df_APICallSize_pivot))],text=Version,mode='text',legendgroup='apicallsize'))
    APICallSizeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APICallSize_pivot)),max(pd.DataFrame.max(df_APICallSize_pivot))],name=Version,mode='lines',legendgroup='apicallsize'))

    df_APICallTime_pivot = pd.pivot_table(APIUsageDF, values = 'TotalSecs',index='Minute10',columns=['CallName'],aggfunc=np.sum)
    APICallTimeTraces = [go.Scatter(x=df_APICallTime_pivot.index,y=df_APICallTime_pivot[col],name=col,legendgroup='apicalltime') for col in df_APICallTime_pivot.columns]
    APICallTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APICallTime_pivot)),max(pd.DataFrame.max(df_APICallTime_pivot))],text=Version,mode='text',legendgroup='apicalltime'))
    APICallTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_APICallTime_pivot)),max(pd.DataFrame.max(df_APICallTime_pivot))],name=Version,mode='lines',legendgroup='apicalltime'))

  else:
    APICallVolumeTraces = []
    APICallSizeTraces = []
    APICallTimeTraces = []
   

  if (PgStatQueryDF.empty != True):
    df_QueryVolume_pivot = pd.pivot_table(PgStatQueryDF, values = 'TotCalls',index=['QueryDate'],columns=['ShortQuery'],aggfunc=np.sum)
    QueryVolumeTraces = [go.Scatter(x=df_QueryVolume_pivot.index,y=df_QueryVolume_pivot[col],name=col,legendgroup='queryvolume') for col in df_QueryVolume_pivot.columns]
    QueryVolumeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_QueryVolume_pivot)),max(pd.DataFrame.max(df_QueryVolume_pivot))],text=Version,mode='text',legendgroup='queryvolume'))
    QueryVolumeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_QueryVolume_pivot)),max(pd.DataFrame.max(df_QueryVolume_pivot))],name=Version,mode='lines',legendgroup='queryvolume'))

    df_SecPerQuery_pivot = pd.pivot_table(PgStatQueryDF, values = 'SecPerCall',index='QueryDate',columns=['ShortQuery'],aggfunc=np.sum)
    SecPerQueryTraces = [go.Scatter(x=df_SecPerQuery_pivot.index,y=df_SecPerQuery_pivot[col],name=col,legendgroup='secperquery') for col in df_SecPerQuery_pivot.columns]
    SecPerQueryTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_SecPerQuery_pivot)),max(pd.DataFrame.max(df_SecPerQuery_pivot))],text=Version,mode='text',legendgroup='secperquery'))
    SecPerQueryTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_SecPerQuery_pivot)),max(pd.DataFrame.max(df_SecPerQuery_pivot))],name=Version,mode='lines',legendgroup='secperquery'))

    df_TotQueryTime_pivot = pd.pivot_table(PgStatQueryDF, values = 'TotTime_ms',index='QueryDate',columns=['ShortQuery'],aggfunc=np.sum)
    TotQueryTimeTraces = [go.Scatter(x=df_TotQueryTime_pivot.index,y=df_TotQueryTime_pivot[col],name=col,legendgroup='totalsec') for col in df_TotQueryTime_pivot.columns]
    TotQueryTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_TotQueryTime_pivot)),max(pd.DataFrame.max(df_TotQueryTime_pivot))],text=Version,mode='text',legendgroup='totalsec'))
    TotQueryTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_TotQueryTime_pivot)),max(pd.DataFrame.max(df_TotQueryTime_pivot))],name=Version,mode='lines',legendgroup='totalsec'))

  else:
    QueryVolumeTraces = []
    SecPerQueryTraces = []
    TotQueryTimeTraces = []

  if (BadFileCountDF.empty != True):
    BadFileCountTraces.append(go.Scatter(x = BadFileCountDF['StatusDateUTC'], y = BadFileCountDF['TotalBadFiles'], name='Total Bad Files',legendgroup='badfiles'))
    BadFileCountTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,max(BadFileCountDF['TotalBadFiles'])],text=Version,mode='text',legendgroup='badfiles'))
    BadFileCountTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[0,max(BadFileCountDF['TotalBadFiles'])],name=Version,mode='lines',legendgroup='badfiles'))

  if (LagTimeDF.empty != True):
    df_LagTime_pivot = pd.pivot_table(LagTimeDF, values='AverageValue',index=['DateTime'],columns=['Action'], aggfunc=np.sum)
    LagTimeTraces = [go.Scatter(x=df_LagTime_pivot.index,y=df_LagTime_pivot[col],name=col,legendgroup='lagtime') for col in df_LagTime_pivot.columns]
    LagTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_LagTime_pivot)),max(pd.DataFrame.max(df_LagTime_pivot))],text=Version,mode='text',legendgroup='lagtime'))
    LagTimeTraces.append(go.Scatter(x=[LastUpgrade,LastUpgrade],y=[min(pd.DataFrame.max(df_LagTime_pivot)),max(pd.DataFrame.max(df_LagTime_pivot))],name=Version,mode='lines',legendgroup='lagtime'))
  else:
    LagTimeTraces = []

  for trace in CpuUsageTraces:
    fig.append_trace(trace,1,1)
  for trace in memoryUsageTraces:
    fig.append_trace(trace,2,1)
  for trace in CpuPerfReportTraces:
    fig.append_trace(trace,3,1)
  for trace in ActualPerfReportTraces:
    fig.append_trace(trace,4,1)
  for trace in APICallVolumeTraces:
    fig.append_trace(trace,5,1)
  for trace in APICallSizeTraces:
    fig.append_trace(trace,6,1)
  for trace in APICallTimeTraces:
    fig.append_trace(trace,7,1)
  for trace in QueryVolumeTraces:
    fig.append_trace(trace,8,1)
  for trace in SecPerQueryTraces:
    fig.append_trace(trace,9,1)
  for trace in TotQueryTimeTraces:
    fig.append_trace(trace,10,1)
  for trace in BadFileCountTraces:
    fig.append_trace(trace,11,1)
  for trace in LagTimeTraces:
    fig.append_trace(trace,12,1)

  fig.update_layout(height=6000, title_text = "Health Check Plots - {}".format(MachineName))

  return fig


def LinkGen(perfFigure, machineName, datetimeStr, perfDataType):

  fileName = machineName + '-' + datetimeStr + '-' + perfDataType + '.html'
  folderName = machineName

  #save figure
  offline.plot(perfFigure, filename = fileName, auto_open=False)

  #move file to bucket w/ multithreading for excellent SPEED
  #!gsutil -m mv Content/$fileName gs://maintenance-build-health-check/$folderName/
  !gsutil -m mv $fileName gs://mygeotab-checkmatedumps/SVROPS-Health-Checks/$folderName/

  #set object acls w/multithreading for excellent SPEED
  #!gsutil -m acl ch -g serveroperations@geotab.com:R gs://maintenance-build-health-check/$folderName/$fileName

  #generate the URL
  fileURL = 'https://storage.cloud.google.com/mygeotab-checkmatedumps/SVROPS-Health-Checks/' + folderName + '/' + fileName

  return fileURL

def GetDateTimeStrFormat():
  currentTimestamp = dt.datetime.now()
  formattedTimestamp = currentTimestamp.strftime("%Y%m%d-%H%M")
  return formattedTimestamp

def threadedPlotting(MachineName):
  time.sleep(random.randint(1,90))
  print("Now starting async plotting for",MachineName)
  fig = getAsyncPlots(MachineName = MachineName)
  fileURL = LinkGen(perfFigure = fig, machineName = MachineName, datetimeStr = GetDateTimeStrFormat(), perfDataType = 'ServerPerformance')
  FileURLs['ServerPerformance'].append(fileURL)
  print("Currently finished server: ", MachineName)
  
  #obtain current stage one server list from instance metadata
def getStageOneServers(majorVersion):
  queryText = """
  SELECT CONCAT("GCEMYGEOTP", (SUBSTR(CName, 3))) as MachineName
  FROM (
  SELECT SPLIT(Name, "-")[OFFSET(0)] as CName
  FROM `geotab-devops.GoogleComputeEngine.Instances_*`
  WHERE _Table_Suffix = format_date('%Y%m%d', current_date())
  AND LOWER(Project) like 'geotab-myfed%'
  AND LOWER(metadata) like "%u'releasechannel', u'value': u'stageone{0}%")
  """

  completeQueryText = queryText.format(majorVersion)
  StageOneServerQuery = bq.Query(completeQueryText)
  StageOneJob = StageOneServerQuery.execute_async(dialect='standard')

  return StageOneJob

if isAuto != False:
  InputServers = getStageOneServers(majorVersion=majorVersion)
  InputServersDF = InputServers.results.to_dataframe()
  serversToPlot = InputServersDF['MachineName']

  machineList = []

  for server in serversToPlot:
    machineList.append(server)

else:
  machineList = serverList

if len(machineList) > 0:
  FileURLs = {
      'ServerPerformance' : []
  }

  threads = []
  for MachineName in machineList:
    process = Thread(target=threadedPlotting, args=[MachineName])
    process.start()
    threads.append(process)
  for process in threads:
    process.join()

else:
  pass
print(FileURLs)