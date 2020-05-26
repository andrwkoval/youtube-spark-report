def query_5(csv_path, spark):
    data = spark.read.csv(csv_path, inferSchema=True, header=True, multiLine=True)
    channels_dict = dict()

    channels = [i.channel_title for i in data.select('channel_title').distinct().collect()]

    for channel in channels:
        print(channel)
        total_trending_days = 0
        videos_days = []

        channel_data = data.where(col("channel_title") == channel)
        videos = [i.video_id for i in channel_data.select('video_id').distinct().collect()]

        for video_id in videos:
            trending_days = channel_data.where(channel_data.video_id == video_id).count()
            video_title = channel_data.where(channel_data.video_id == video_id).select("title").distinct().collect()[0][
                0]

            videos_days.append({"video_id": video_id,
                                "video_title": video_title,
                                "trending_days": trending_days})
            total_trending_days += trending_days

        channels_dict[channel] = {"channel_name": channel,
                                  "total_trending_days": total_trending_days,
                                  "videos_days": videos_days}

    return sorted(channels_dict.items(), key=lambda x: x[1]["total_trending_days"], reverse=True)[:10]
