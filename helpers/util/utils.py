import pyspark


def fetch_sql_table(spark: pyspark.sql.SparkSession,
                    db: str,
                    db_table: str,
                    host: str = "localhost",
                    port: int = 3307,
                    user: str = "root",
                    password: str = "admin"):
    if not spark or not db or not db_table:
        raise Exception('spark || db || db_table is undefined')
    return spark.read.format("jdbc").option("url", f"jdbc:mysql://{host}:{port}/{db}?zeroDateTimeBehavior=convertToNull") \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", f"{db}.{db_table}") \
        .option("user", user).option("password", password).load()


def write_sql_table(df,
                    db: str,
                    db_table: str,
                    host: str = "localhost",
                    port: int = 3307,
                    user: str = "root",
                    password: str = "admin",
                    mode: str = "overwrite"):

    if not df or not db or not db_table:
        raise Exception('spark || db || db_table is undefined')
    df.write.format('jdbc').options(
        url=f'jdbc:mysql://{host}:{port}/{db}',
        driver='com.mysql.jdbc.Driver',
        dbtable=db_table,
        user=user,
        password=password).mode(mode).save()
