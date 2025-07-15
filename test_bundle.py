import requests, time, urllib.parse

proxy = "http://kdjkprokmp:xfP752jfzb@185.2.212.223:51523"
t0 = time.perf_counter()
try:
    r = requests.get("https://api.ipify.org", proxies={"http": proxy, "https": proxy}, timeout=10)
    print("✅", r.text, f"{time.perf_counter()-t0:.2f}s")
except Exception as e:
    print("❌", e)