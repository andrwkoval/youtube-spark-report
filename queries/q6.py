import json

def category_names(json_path):
    with open(json_path, "r") as f:
        categories = json.load(f)

    return {category["id"]: category["snippet"]["title"] for category in categories}

def query_6(csv_path, json_path, spark):
    data = spark.read.csv(csv_path, inferSchema=True, header=True, multiLine=True)
    categories_result = {}

    categories_names = category_names(json_path)

    categories = [i.category_id for i in data.select("category_id").distinct().select * ()]

    for category in categories:
        print(category)
        videos = []

        category_data = data.where(col("category_id") == category)
        video_data = [i.video_id for i in category_data.select('video_id').distinct().collect()]

        for video_id in video_data:
            video_title = category_data.where(category_data.video_id == video_id) \
                .select("title").distinct().collect()[0][0]
            video_views = category_data.where(category_data.video_id == video_id) \
                .select("views").distinct().collect()[0][0]
            video_likes = category_data.where(category_data.video_id == video_id) \
                .select("likes").distinct()[0][0]
            video_dislikes = category_data.where(category_data.video_id == video_id) \
                .select("dislikes").distinct()[0][0]
            like_dislike_ratio = video_likes / video_dislikes

            videos.append({
                "video_id": video_id,
                "video_title": video_title,
                "ratio_likes_dislikes": like_dislike_ratio,
                "Views": video_views
            })

        categories_result[category] = {
            "category_id": category,
            "category_name": categories_names[category],
            "videos": sorted(videos, key=lambda x: x["ratio_likes_dislikes"], reverse=True)[:10]
        }

    return categories_result

