from pyspark.sql.functions import  col, collect_list
import datetime

def add_days(start, days_num):
  # 60 seconds in 60 min
  one_hour = 60 * 60
  
  # get seconds from this date
  curr_secs = convert_to_datetime(start)

  # count a days_num after that date
  end_secs = curr_secs + one_hour * 24 * days_num

  return datetime.datetime.fromtimestamp(end_secs).strftime("%y.%d.%m")


def convert_to_datetime(date):
  # get year, month and day from the date
  year, day, month = date.split(".")

  return (datetime.datetime(int("20" + year), int(month), int(day)) - datetime.datetime(1970, 1, 1)).total_seconds()


def count_views(data, video_id):
  video_data = data.filter(data.video_id == video_id)
  trending_days = video_data.select(collect_list('trending_date')).first()[0]

  if len(trending_days) < 2:
    return None

  first, last = trending_days[0], trending_days[-1]
  
  first_views = video_data.where(video_data.trending_date == first).select(collect_list('views')).first()[0][0]
  last_views = video_data.where(video_data.trending_date == last).select(collect_list('views')).first()[0][0]
  
  return last_views - first_views, video_id



def total_category_views(data, category_id):
  df = data.where(col("category_id") == category_id)

  video_ids = [i.video_id for i in df.select('video_id').distinct().collect()]
  total_views = 0

  video_lst = []

  for video_id in video_ids:
    result = count_views(df, video_id)
    
    if result:
      total_views += result[0]
      video_lst.append(result[1])
  
  return total_views, video_lst


def is_in_bounds(date, start, end):
  return convert_to_datetime(start) <= convert_to_datetime(date) < convert_to_datetime(end)

import json

def get_category_names(path2json):
  with open(path2json, "r") as file:
    category_json = json.load(file)
  
  id2category = dict()

  for category in category_json["items"]:
    id2category[category["id"]] = category["snippet"]["title"]

  return id2category



def query_2(csv_path, json_path):
  data = spark.read.csv(csv_path , inferSchema=True, header=True, multiLine=True)
  id2category = get_category_names(json_path)


  # get all trending dates
  trending_dates = [i.trending_date for i in data.select('trending_date').distinct().collect()]
  
  weeks = dict()

  start = trending_dates[0]
  end = add_days(start, 7)

  while convert_to_datetime(start) < convert_to_datetime(trending_dates[-1]):
    print(start, end)
    # find top category
    week_dates = [i for i in trending_dates if is_in_bounds(i, start, end)]
    week_data = data.where(col("trending_date").isin(week_dates))
    
    week_categories = [i.category_id for i in week_data.select('category_id').distinct().collect()]
    category2views = dict()

    for category in week_categories:
      category_result = total_category_views(week_data, category)
      category2views[category] = category_result
    
    top_category = sorted(category2views.items(), key=lambda x: x[1][0], reverse=True)[0]

    weeks[(start, end)] = (top_category[0], category2views[top_category[0]])

    start = end
    end = add_days(end, 7)
  
  result = dict()

  for week in weeks:
    result[week] = {"start_date": week[0],
                    "end_date": week[1],
                    "category_id": weeks[week][0],
                    "category_name": id2category[weeks[week][0]],
                    "number_of videos": len(weeks[week][1][1]),
                    "total_views": weeks[week][1][0],
                    "video_ids": weeks[week][1][1]}
  
  return result


if __name__ == '__main__':
    a = col("category_id")