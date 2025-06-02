import requests
import json
import base64

API_URL = "https://your-api-id.execute-api.region.amazonaws.com/dev/query"  # 替换为你的实际地址

# 1. byTagsWithCount
def test_by_tags_with_count():
    payload = {
        "queryType": "byTagsWithCount",
        "tags": {
            "crow": 2,
            "pigeon": 1
        }
    }
    send(payload)

# 2. byTagsOnly
def test_by_tags_only():
    payload = {
        "queryType": "byTagsOnly",
        "tags": ["sparrow", "peacock"]
    }
    send(payload)

# 3. byThumbUrl
def test_by_thumbnail_url():
    payload = {
        "queryType": "byThumbUrl",
        "thumbnail_url": "https://your-bucket.s3.amazonaws.com/img-thumb.jpg"  # 替换为测试用URL
    }
    send(payload)

# 4. predictAndSearch
def test_predict_and_search():
    with open("test_images/sparrow_3.jpg", "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "queryType": "predictAndSearch",
        "image_base64": encoded
    }
    send(payload)

# 5. manualTagEdit
def test_manual_tag_edit():
    payload = {
        "queryType": "manualTagEdit",
        "url": [
            "https://your-bucket.s3.amazonaws.com/img-thumb.jpg"
        ],
        "operation": 1,
        "tags": [
            "crow,1",
            "pigeon,2"
        ]
    }
    send(payload)

# 通用请求方法
def send(payload):
    headers = {"Content-Type": "application/json"}
    response = requests.post(API_URL, headers=headers, data=json.dumps(payload))
    print("Status:", response.status_code)
    print("Response:", response.json())

if __name__ == "__main__":
    # 根据需要取消注释对应测试项
    # test_by_tags_with_count()
    # test_by_tags_only()
    # test_by_thumbnail_url()
    # test_predict_and_search()
    # test_manual_tag_edit()
    print("Choose a test to run by uncommenting the corresponding function call.")
