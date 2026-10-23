import io
import os
import urllib.request
import zipfile

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
DEST = os.path.join("data", "landing", "movielens")


def main():
    os.makedirs(DEST, exist_ok=True)
    print("Đang tải", URL)
    with urllib.request.urlopen(URL) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        z.extractall(DEST)
    print("Đã giải nén vào", DEST)


if __name__ == "__main__":
    main()