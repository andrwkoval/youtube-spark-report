from pyspark.sql.functions import collect_list, array, desc, col, floor, row_number
import pyspark.sql.functions as functions


def q4(data):
    channels = data.select(data.channel_title, data.video_id, data.trending_date_str, data.views)

    channels = channels.groupby(["channel_title", "video_id"]) \
        .agg(functions.max(channels.views)
             .alias("views"),
             functions.min(channels.trending_date_str)
             .alias("start_date"),
             functions.max(channels.trending_date_str)
             .alias("end_date"))

    channels = channels.withColumn("views_str", channels.views.cast("string")) \
        .groupby("channel_title") \
        .agg(functions.sum("views").alias("total_views"),
             collect_list(array(["video_id", "views_str"])).alias("video_views"),
             functions.min(channels.start_date).alias("start_date"),
             functions.max(channels.end_date).alias("end_date")) \
        .orderBy("total_views", ascending=False)

    return channels
