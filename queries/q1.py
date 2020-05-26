from pyspark.sql.functions import countDistinct, collect_list, array, udf, floor, row_number
from pyspark.sql.types import IntegerType, ArrayType


def q1(data):
    latest_day = udf(lambda x: [int(x) for x in max(x, key=lambda lst: lst[0])[1:]], ArrayType(IntegerType()))

    trend_videos = data.groupBy(["video_id", "title", "description", "channel_title"]) \
        .agg(collect_list(array('trending_date', "views", "likes", "dislikes")) \
             .alias("trending_days"), countDistinct("trending_date") \
             .alias("in_trends")) \
        .orderBy("in_trends", ascending=False)

    trend_vids = trend_videos.withColumn("latest_day", latest_day(trend_videos.trending_days))
    trend_vids = trend_vids.withColumn("latest_views", trend_vids.latest_day[0]) \
        .withColumn("latest_likes", trend_vids.latest_day[1]) \
        .withColumn("latest_dislikes", trend_vids.latest_day[2])

    trend_vids = trend_vids.drop("latest_day")
    return trend_vids
