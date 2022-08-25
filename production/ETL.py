from pyspark.sql.functions import percentile_approx


def level_to_num(df):
    """
    turn string level string to numeric level
    return pyspark.DataFrame
    """
    return df.replace(to_replace=['EN', 'MI', 'SE', 'EX'], value=['2', '1', '3', '4'])



def select_highly_paid_remote_job(df):
    """
    select salary_in_job > q75 and remote ratio == 100
    """
    q75 = df.select(percentile_approx("salary_in_usd", [0.75], 100000000).alias("quantiles")).collect()[0].quantiles[0]
    df = df.filter(df.salary_in_usd > q75)
    df = df.filter(df.remote_ratio == 100)
    return df