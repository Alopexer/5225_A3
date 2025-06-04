import json
import boto3
from boto3.dynamodb.conditions import Attr, And
from functools import reduce
import decimal
# from birds_detection import predict_tags
# import base64

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("ImageTagsTable")  # 修改为真实表名
s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        if event.get("httpMethod") != "POST":
            return error(405, "Only POST allowed")

        raw_body = event.get("body", "{}")

        if isinstance(raw_body, str):
            data = json.loads(raw_body)
        else:
            data = raw_body

        action = data.get("action")

        if action == "search":
            return query_by_tags_with_count(data)
        elif action == "fuzzy":
            return query_by_tags_fuzzy(data)
        elif action == "thumbnail":
            return query_by_thumbnail(data)
        elif action == "upload_query":
            return query_by_uploaded_image(data)
        elif action == "tag_edit":
            return modify_tags(data)
        elif action == "delete":
            return delete_records(data)
        elif action == "insert":
            return insert_item(data)
        else:
            return error(400, "Invalid or missing action")

    except Exception as e:
        return error(500, str(e))

# ---------------- Query Handlers ---------------- #

def query_by_tags_with_count(data):
    cond = data.get("tags", {})
    if not cond:
        return error(400, "Missing tags")

    response = table.scan()
    matched = []

    for item in response.get("Items", []):
        item_tags = item.get("tags", {})
        all_matched = True

        for k, v in cond.items():
            required = int(v["N"])
            actual = int(item_tags.get(k, 0))  
            if actual < required:
                all_matched = False
                break

        if all_matched:
            matched.append(item.get("s3_url"))

    return success({"Links": matched})

def query_by_tags_fuzzy(data):
    tags = data.get("tags", [])
    if not tags:
        return error(400, "Missing tags")

    matched = []
    response = table.scan()
    for item in response.get("Items", []):
        item_tags = item.get("tags", {})
        if any(tag in item_tags for tag in tags):
            url = item.get("s3_url")
            if url:  # 只添加非空链接
                matched.append(url)

    return success({"Links": matched})


def query_by_thumbnail(data):
    thumb = data.get("thumbnail")
    if not thumb:
        return error(400, "Missing thumbnail")

    response = table.scan(FilterExpression=Attr("thumbnail_url").eq(thumb))
    items = response.get("Items", [])
    return success(items[0] if items else {})


# def query_by_uploaded_image(data):
#     image_base64 = data.get("image")
#     if not image_base64:
#         return error(400, "Missing base64-encoded image")

#     try:
#         image_bytes = base64.b64decode(image_base64)
#     except Exception:
#         return error(400, "Base64 decode failed")

#     try:
#         tags = predict_tags(image_bytes)  
#         if not tags:
#             return error(500, "No tags predicted")
#     except Exception as e:
#         return error(500, f"Inference error: {str(e)}")

#     # ✅ 在 DynamoDB 中模糊查询
#     response = table.scan()
#     matched = []
#     for item in response.get("Items", []):
#         item_tags = item.get("tags", {})
#         if any(tag in item_tags for tag in tags):
#             matched.append(item.get("s3-url"))

#     return success({
#         "inferred_tags": tags,
#         "Links": matched
#     })

def modify_tags(data):
    urls = data.get("urls", [])
    tags = data.get("tags", [])  # e.g., ["Crow,1", "Pigeon,2"]
    operation = data.get("operation")  # 1 for add, 0 for delete

    for url in urls:
        # 查找该 thumbnail_url 对应的记录（反向获取 filename）
        response = table.scan(FilterExpression=Attr("thumbnail_url").eq(url))
        items = response.get("Items", [])
        if not items:
            continue

        item = items[0]
        filename = item["filename"]  # filename作为主键
        original = item.get("tags", {})

        # 更新标签
        for tag_entry in tags:
            name, val = tag_entry.split(",")
            if operation == 1:
                original[name] = int(val)
            elif operation == 0 and name in original:
                del original[name]

        # 执行更新操作
        table.update_item(
            Key={"filename": filename},
            UpdateExpression="SET tags = :t",
            ExpressionAttributeValues={":t": original}
        )

    return success({"message": "Tags updated"})

def delete_records(data):
    urls = data.get("urls", [])
    deleted = []

    for url in urls:
        response = table.scan(FilterExpression=Attr("thumbnail_url").eq(url))
        items = response.get("Items", [])
        if not items:
            continue

        item = items[0]
        filename = item["filename"]         
        thumb_url = item.get("thumbnail_url")
        s3_url = item.get("s3-url")

        # 删除 S3 文件
        for s3_path in [thumb_url, s3_url]:
            if s3_path:
                bucket, key = parse_s3_url(s3_path)
                try:
                    s3.delete_object(Bucket=bucket, Key=key)
                except:
                    continue

        # 删除数据库记录（按主键 filename）
        table.delete_item(Key={"filename": filename})
        deleted.append(filename)

    return success({"deleted": deleted})

def insert_item(data):
    required_fields = ["filename", "file_type", "s3_url", "tags", "thumbnail_url", "timestamp", "uploader"]
    if not all(k in data for k in required_fields):
        return error(400, "Missing required fields")

    # 构造 DynamoDB item（确保数值类型用 Decimal）
    item = {
        "filename": data["filename"],
        "file_type": data["file_type"],
        "s3_url": data["s3_url"],
        "tags": {k: decimal.Decimal(str(v)) for k, v in data["tags"].items()},
        "thumbnail_url": data["thumbnail_url"],
        "timestamp": data["timestamp"],
        "uploader": data["uploader"]
    }

    table.put_item(Item=item)
    return success({"message": "Item inserted", "filename": data["filename"]})

# ---------------- Utils ---------------- #

def parse_s3_url(s3_url):
    # e.g., https://bucket-name.s3.amazonaws.com/path/to/file.jpg
    parts = s3_url.split("/")
    bucket = parts[2].split(".")[0]
    key = "/".join(parts[3:])
    return bucket, key

def success(body):
    return {
        "statusCode": 200,
        "body": json.dumps(body, cls=DecimalEncoder),
        "headers": cors_headers()
    }

def error(code, message):
    return {
        "statusCode": code,
        "body": json.dumps({"error": message}, cls=DecimalEncoder),
        "headers": cors_headers()
    }

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Allow-Headers": "*"
    }
