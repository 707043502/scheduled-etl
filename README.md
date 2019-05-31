# scheduled-etl

### 计划

**\\src\\** 部分
- 1.找一个公开数据源
- 2.将RAW数据导入AWS S3
- 3.利用pyspark + matplotlib 做简单可视化

**\\etc\\profile\\** 部分
- 1.放配置文件

**\\scripts\\** 部分
- 1.利用 `python redshiftServer.py --start/stop`控制redshiftServer启/停
- 2.利用 `airflow` 按时间调度ETL任务。

尝试：
- 在dev分支上面做开发
