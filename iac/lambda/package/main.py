import json
import boto3
import base64
import os
import uuid
import cv2 as cv
from boto3.dynamodb.conditions import Attr
from birds_detection import get_species_list  #  birds_detection.py 添加此函数

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('BirdMediaMetadata')  # 替换成真实的表名

def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        query_type = body.get("queryType")

        if query_type == "byTagsWithCount":
            return query_by_tags_with_count(body)
        elif query_type == "byTagsOnly":
            return query_by_tags_only(body)
        elif query_type == "byThumbUrl":
            return query_by_thumbnail_url(body)
        elif query_type == "manualTagEdit":
            return update_tags(body)
        elif query_type == "predictAndSearch":
            return predict_and_search(body)
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid queryType."})
            }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

# 查询类型 1: 标签 + 数量过滤
def query_by_tags_with_count(body):
    request_tags = body.get("tags", {})
    if not request_tags:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing tags"})}

    response = table.scan()
    results = []

    for item in response["Items"]:
        file_tags = item.get("tags", {})
        if all(file_tags.get(tag, 0) >= count for tag, count in request_tags.items()):
            results.append(item.get("thumbnail_url") or item.get("s3_url"))

    return {"statusCode": 200, "body": json.dumps({"results": results})}

# 查询类型 2: 任意标签（不带数量）
def query_by_tags_only(body):
    tag_list = body.get("tags", [])
    if not tag_list:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing tag list"})}

    response = table.scan()
    results = []

    for item in response["Items"]:
        file_tags = item.get("tags", {})
        if any(tag in file_tags for tag in tag_list):
            results.append(item.get("thumbnail_url") or item.get("s3_url"))

    return {"statusCode": 200, "body": json.dumps({"results": results})}

# 查询类型 3: 通过 thumbnail_url 返回 s3_url
def query_by_thumbnail_url(body):
    thumb_url = body.get("thumbnail_url")
    if not thumb_url:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing thumbnail_url"})}

    response = table.scan(FilterExpression=Attr("thumbnail_url").eq(thumb_url))
    items = response.get("Items", [])

    if not items:
        return {"statusCode": 404, "body": json.dumps({"error": "Thumbnail not found"})}

    return {"statusCode": 200, "body": json.dumps({"s3_url": items[0].get("s3_url")})}

# 查询类型 4: 上传图像临时识别 → 查找包含相同标签的文件
def predict_and_search(body):
    image_data = body.get("image_base64")
    if not image_data:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing base64 image"})}

    temp_image_path = f"/tmp/query_{uuid.uuid4().hex}.jpg"
    with open(temp_image_path, "wb") as f:
        f.write(base64.b64decode(image_data))

    species = get_species_list(temp_image_path)
    if not species:
        return {"statusCode": 200, "body": json.dumps({"results": []})}

    response = table.scan()
    results = []

    for item in response["Items"]:
        file_tags = item.get("tags", {})
        if all(sp in file_tags for sp in species):
            results.append(item.get("thumbnail_url") or item.get("s3_url"))

    return {"statusCode": 200, "body": json.dumps({"results": results, "detected_species": species})}

# 查询类型 5: 批量增删标签
def update_tags(body):
    urls = body.get("url", [])
    tags = body.get("tags", [])  # 格式: ["crow,1", "pigeon,2"]
    operation = int(body.get("operation", 1))

    parsed_tags = {}
    for tag in tags:
        try:
            name, count = tag.strip().split(",")
            parsed_tags[name.strip()] = int(count)
        except:
            continue

    updated = []

    for url in urls:
        response = table.scan(FilterExpression=Attr("thumbnail_url").eq(url))
        items = response.get("Items", [])
        if not items:
            continue
        item = items[0]
        file_id = item["file_id"]
        file_tags = item.get("tags", {})

        if operation == 1:
            # 添加标签
            for k, v in parsed_tags.items():
                file_tags[k] = file_tags.get(k, 0) + v
        else:
            # 删除标签
            for k in parsed_tags:
                if k in file_tags:
                    file_tags.pop(k)

        table.update_item(
            Key={"file_id": file_id},
            UpdateExpression="SET tags = :t",
            ExpressionAttributeValues={":t": file_tags}
        )
        updated.append(url)

    return {"statusCode": 200, "body": json.dumps({"updated": updated})}