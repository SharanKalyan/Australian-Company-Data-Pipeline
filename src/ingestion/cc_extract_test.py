import requests

r = requests.get(
    "https://index.commoncrawl.org/CC-MAIN-2025-13-index?url=*.au&output=json&page=0",
    timeout=30
)

print(r.status_code)