#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
斗鱼原画源定时刷新器（GitHub Actions / 本地定时任务 通用）

作用：把 iptv-api 结果里的斗鱼转流线路（720P 预览流），替换成斗鱼官方原画直链，
      输出成一个独立的 m3u。配合每 10 分钟跑一次，即可抵消"直链约 20~30 分钟过期"的问题。

用法：
  python douyu_refresh.py --src https://ch1026.github.io/iptv-api/result.m3u --out douyu.m3u
  python douyu_refresh.py --src result.m3u --out douyu.m3u --merge full.m3u --report r.csv

参数：
  --src       源 m3u（本地路径或 http/https URL），默认取 iptv-api 结果地址
  --out       输出的 m3u 文件（只含斗鱼，独立订阅用）
  --merge     额外输出【完整合并版】m3u：源里非斗鱼线路原样保留、斗鱼线路换成原画直链。
              这一版可直接当主订阅用，斗鱼新鲜度由本任务保证，无需依赖主仓库刷新频率。
  --local     可选，额外输出 iptv-api 本地源/白名单格式（频道名,直链）
  --report    可选，输出完整扫描报告 CSV
  --min-width 最小宽度门槛，默认 1920（低于此值的房间会被剔除）
  --no-verify 跳过真流校验。默认会抓 FLV 头实测分辨率，因为斗鱼档位名存在虚标
              （实测有标注 1080P、实际只推 720P 的房间）
  --cdn       斗鱼 CDN 参数，默认 hw-h5（必须显式指定，否则移动网络下会被调度到
              edgesrv.com:8443 边缘节点，导致 TLS 握手失败）
  --limit     只处理前 N 个房间（调试用）
  --workers   并发数，默认 6

退出码：
  0 正常；2 成功率过低（低于 30%）——此时不会写出文件，
  以免把上一轮的好结果覆盖成空文件（Actions 里可据此中断并保留旧产物）。
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import hashlib
import json
import os
import re
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
DID = "10000000000000000000000000001501"
DOUYU_LINE = re.compile(r"live\.ottiptv\.cc/douyu/(\d+)")
# 直链主机必须是斗鱼自家 CDN；出现 edgesrv 说明被调度到了移动边缘节点，需要丢弃
HOST_BAD = re.compile(r"edgesrv", re.I)

_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def http(url, data=None, headers=None, timeout=15):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h)
    return _OPENER.open(req, timeout=timeout)


def parse_resolution(name: str):
    """从档位名解析分辨率，如 '原画1080P30' -> (1920,1080)，'原画4K60' -> (3840,2160)"""
    if not name:
        return 0, 0
    m = re.search(r"(\d{3,4})\s*[xX×*]\s*(\d{3,4})", name)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d{3,4})\s*[pP](?![0-9a-zA-Z])", name)
    if m:
        h = int(m.group(1))
        return int(round(h * 16 / 9 / 2)) * 2, h
    up = name.upper()
    if "4K" in up or "2160" in up:
        return 3840, 2160
    if "2K" in up or "1440" in up:
        return 2560, 1440
    if "1080" in name:
        return 1920, 1080
    return 0, 0


def flv_resolution(url: str, timeout: int = 20):
    """实抓 FLV 流的 onMetaData，返回真实 (宽, 高, 视频码率kbps)。

    必要性：斗鱼返回的档位名存在虚标（实测有标注 1080P、实际只推 720P 的房间），
    只信档位名会把低清源混进结果，所以这里做一次真流校验。
    """
    try:
        resp = http(url, headers={"User-Agent": "Lavf/58.76.100"}, timeout=timeout)
        data = resp.read(260000)
    except Exception:  # noqa: BLE001
        return 0, 0, 0

    def num_after(tag: bytes):
        i = data.find(tag)
        while i >= 0:
            j = i + len(tag)
            if data[j:j + 1] == b"\x00" and j + 9 <= len(data):
                return struct.unpack(">d", data[j + 1:j + 9])[0]
            i = data.find(tag, i + 1)
        return None

    w = num_after(b"width")
    h = num_after(b"height")
    vk = num_after(b"videodatarate")
    return (int(w) if w else 0, int(h) if h else 0, int(vk) if vk else 0)


class Douyu:
    """斗鱼取流（2024 后新版签名：密钥包 + 迭代 MD5）"""

    KEY_URL = "https://www.douyu.com/wgapi/livenc/liveweb/websec/getEncryption?did=" + DID
    PLAY_URL = "https://www.douyu.com/lapi/live/getH5PlayV1/%s"

    def __init__(self, cdn: str = "hw-h5"):
        self.cdn = cdn
        self._kd = None
        self._lock = threading.Lock()

    def key(self, force: bool = False):
        with self._lock:
            if self._kd is not None and not force:
                return self._kd
            last = None
            for _ in range(4):
                try:
                    raw = http(self.KEY_URL, timeout=20).read().decode("utf-8", "ignore")
                    j = json.loads(raw[raw.find("{"):raw.rfind("}") + 1])
                    if j.get("error") == 0 and j.get("data"):
                        self._kd = j["data"]
                        return self._kd
                    last = "%s %s" % (j.get("error"), j.get("msg"))
                except Exception as e:  # noqa: BLE001
                    last = str(e)[:80]
                time.sleep(1.5)
            raise RuntimeError("获取签名密钥失败: %s" % last)

    @staticmethod
    def _sign(rid: str, ts: int, kd: dict) -> str:
        # is_special=1 时不拼 rid+ts，否则拼
        origin = "" if int(kd.get("is_special") or 0) == 1 else "%s%d" % (rid, ts)
        u = kd["rand_str"]
        a = kd["key"]
        for _ in range(int(kd["enc_time"])):
            u = md5(u + a)
        return md5(u + a + origin)

    def play(self, rid: str, rate: int = 0, retry: int = 2) -> dict:
        """返回 {error,msg,url,rate_name,bit,width,height}"""
        last = {"error": "EXC", "msg": "unknown"}
        for attempt in range(retry + 1):
            ts = int(time.time())
            try:
                kd = self.key(force=(attempt > 0))
                auth = self._sign(rid, ts, kd)
                body = (
                    "enc_data=%s&tt=%d&did=%s&auth=%s&cdn=%s&ver=Douyu_new"
                    "&rate=%d&hevc=0&fa=0&ive=0"
                    % (urllib.parse.quote(str(kd["enc_data"]), safe=""),
                       ts, DID, auth, self.cdn, rate)
                ).encode()
                raw = http(
                    self.PLAY_URL % rid, data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded",
                             "Referer": "https://www.douyu.com/%s" % rid},
                    timeout=20,
                ).read().decode("utf-8", "ignore")
                j = json.loads(raw[raw.find("{"):raw.rfind("}") + 1])
                if j.get("error") != 0:
                    last = {"error": j.get("error"), "msg": j.get("msg")}
                    if attempt < retry:
                        time.sleep(1.2)
                        continue
                    return last
                d = j.get("data") or {}
                url = (d.get("rtmp_url") or "").rstrip("/") + "/" + (d.get("rtmp_live") or "")
                mr = d.get("multirates") or []
                rate_name = (mr[0].get("name") if mr else "") or ""
                bit = (mr[0].get("bit") if mr else 0) or 0
                w, h = parse_resolution(rate_name)
                return {"error": 0, "msg": "", "url": url, "rate_name": rate_name,
                        "bit": bit, "width": w, "height": h}
            except Exception as e:  # noqa: BLE001
                last = {"error": "EXC", "msg": str(e)[:90]}
                if attempt < retry:
                    time.sleep(1.2)
        return last


def read_source(src: str):
    if re.match(r"^https?://", src, re.I):
        return http(src, timeout=30).read().decode("utf-8", "ignore").splitlines()
    with open(src, encoding="utf-8", errors="ignore") as f:
        return f.read().splitlines()


def collect_rooms(lines):
    """从 m3u 中抽取斗鱼房间：保留上一行的 #EXTINF 原样，便于沿用分组与台标"""
    rooms = []
    for i, ln in enumerate(lines):
        m = DOUYU_LINE.search(ln)
        if not m:
            continue
        extinf = None
        if i > 0 and lines[i - 1].lstrip().startswith("#EXTINF"):
            extinf = lines[i - 1].strip()
        name = extinf.split(",")[-1].strip() if extinf else m.group(1)
        rooms.append({"rid": m.group(1), "name": name, "extinf": extinf})
    return rooms


def main():
    ap = argparse.ArgumentParser(description="斗鱼原画源定时刷新器")
    ap.add_argument("--src", default="https://ch1026.github.io/iptv-api/result.m3u",
                    help="源 m3u 路径或 URL")
    ap.add_argument("--out", default="douyu.m3u", help="输出的 m3u 文件")
    ap.add_argument("--local", default=None, help="额外输出 iptv-api 本地源格式（频道名,直链）")
    ap.add_argument("--report", default=None, help="输出完整扫描报告 CSV")
    ap.add_argument("--merge", default=None,
                    help="输出【完整合并版】m3u：源里的非斗鱼线路原样保留，"
                         "斗鱼线路替换为原画直链。用于替代主订阅，避免依赖主仓库刷新频率。")
    ap.add_argument("--no-verify", action="store_true",
                    help="跳过实际分辨率校验（默认会抓 FLV 头实测，防止斗鱼档位名虚标）")
    ap.add_argument("--min-width", type=int, default=1920, help="最小宽度，默认 1920")
    ap.add_argument("--cdn", default="hw-h5", help="斗鱼 CDN 参数，默认 hw-h5")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 个房间（调试用）")
    ap.add_argument("--workers", type=int, default=6, help="并发数，默认 6")
    ap.add_argument("--min-success-ratio", type=float, default=0.3,
                    help="最低成功率，低于则退出码 2 且不写文件")
    args = ap.parse_args()

    t0 = time.time()
    print("源      :", args.src)
    lines = read_source(args.src)
    rooms = collect_rooms(lines)
    print("斗鱼房间:", len(rooms), "个")
    if not rooms:
        print("!! 源里没有斗鱼线路，终止（不覆盖已有文件）")
        return 2

    rids = []
    for r in rooms:
        if r["rid"] not in rids:
            rids.append(r["rid"])
    if args.limit:
        rids = rids[:args.limit]
    print("去重房间:", len(rids), "个 | CDN:", args.cdn, "| 并发:", args.workers)
    print("-" * 78)

    dy = Douyu(cdn=args.cdn)
    # 先取一次密钥，避免 200 个线程同时抢
    try:
        dy.key()
        print("签名密钥: 已获取")
    except Exception as e:  # noqa: BLE001
        print("签名密钥: 获取失败 ->", e)
        return 2

    results = {}
    done = 0
    lock = threading.Lock()

    def work(rid):
        nonlocal done
        r = dy.play(rid)
        with lock:
            done += 1
            if done % 25 == 0 or done == len(rids):
                print("  进度 %d/%d" % (done, len(rids)))
        return rid, r

    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for rid, r in ex.map(work, rids):
            results[rid] = r

    ok, low, fail = [], [], []
    for rid in rids:
        r = results[rid]
        if r.get("error"):
            fail.append((rid, r))
        elif HOST_BAD.search(r.get("url", "")):
            fail.append((rid, {"error": "BADHOST", "msg": "被调度到 edgesrv 边缘节点"}))
        elif r.get("width", 0) < args.min_width:
            low.append((rid, r))
        else:
            ok.append((rid, r))

    # 实测校验：斗鱼档位名存在虚标（实测有标注 1080P 但只推 720P 的房间），
    # 只信档位名会把低清源混进结果，所以抓一次真流确认。
    if ok and not args.no_verify:
        print("实测校验: %d 条（读 FLV 头确认真实分辨率）" % len(ok))
        verified, false_label = [], 0
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for (rid, r), (w, h, bit) in zip(ok, ex.map(
                    lambda x: flv_resolution(x[1]["url"]), ok)):
                if w and w < args.min_width:
                    false_label += 1
                    bad = dict(r)
                    bad["verified"] = "档位名 %s 虚标，实测 %dx%d" % (
                        r.get("rate_name") or "?", w, h)
                    low.append((rid, bad))
                else:
                    good = dict(r)
                    good["verified"] = ("实测 %dx%d %dkbps" % (w, h, bit)) if w else "未校验成功（探测超时）"
                    verified.append((rid, good))
        ok = verified
        print("  校验后达标: %d 条 | 拦截虚标: %d 条" % (len(ok), false_label))

    print("-" * 78)
    print("原画达标 : %d" % len(ok))
    print("低于门槛 : %d" % len(low))
    print("取流失败 : %d" % len(fail))
    if fail:
        from collections import Counter
        print("失败原因 :", dict(Counter(str(v.get("error")) + " " + str(v.get("msg"))[:24]
                                        for _, v in fail).most_common(4)))

    if not ok or len(ok) < len(rids) * args.min_success_ratio:
        print("!! 成功率过低（%d/%d），判定为接口异常，不写出文件" % (len(ok), len(rids)))
        return 2

    # 组装输出：沿用原始 #EXTINF，保证分组/台标与主订阅一致
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    out = ["#EXTM3U x-tvg-url=\"https://ch1026.github.io/iptv-api/epg.xml\"",
           "# 生成时间: %s (UTC+8) | 斗鱼直链有效期约 20~30 分钟，靠定时重跑保持新鲜" % stamp]
    local = ["# iptv-api 本地源 / 白名单格式：频道名,直链",
             "# 直链约 20~30 分钟过期，需由定时任务重新生成"]
    report = [("频道名", "房间号", "状态", "规格", "宽度", "高度", "码率kbps", "直链")]
    written = set()
    seen_local = set()
    good_map = {rid: r for rid, r in ok}

    for room in rooms:
        rid, name = room["rid"], room["name"]
        if args.limit and rid not in results:
            continue
        r = good_map.get(rid)
        if not r:
            src = results.get(rid) or {}
            reason = src.get("verified") or src.get("rate_name") or str(src.get("error") or "未取流")
            report.append((name, rid, "剔除", reason, 0, 0, 0, ""))
            continue
        out.append(room["extinf"] or "#EXTINF:-1,%s" % name)
        out.append(r["url"])
        written.add(rid)
        report.append((name, rid, "OK", r["rate_name"], r["width"], r["height"], r["bit"], r["url"]))
        if name not in seen_local:
            seen_local.add(name)
            local.append("%s,%s" % (name, r["url"]))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print("已写出   :", args.out, "(%d 个频道)" % len(written))

    if args.local:
        with open(args.local, "w", encoding="utf-8") as f:
            f.write("\n".join(local) + "\n")
        print("已写出   :", args.local, "(%d 条)" % (len(local) - 2))
    if args.report:
        with open(args.report, "w", encoding="utf-8-sig", newline="") as f:
            csv.writer(f).writerows(report)
        print("已写出   :", args.report)

    # 完整合并版：源 m3u 里的非斗鱼线路原样保留，斗鱼线路替换为原画直链。
    # 用途：直接替代主订阅地址，这样斗鱼源的新鲜度由本任务（每 10 分钟）保证，
    # 不受主仓库 12 小时刷新频率的限制，也无需碰主仓库的白名单配置。
    if args.merge:
        merged, noted = [], False
        for ln in lines:
            if ln.startswith("#EXTM3U") and not noted:
                merged.append(ln)
                merged.append("# 斗鱼线路已替换为官方原画直链 | 生成时间: %s (UTC+8)" % stamp)
                noted = True
                continue
            m = DOUYU_LINE.search(ln)
            if not m:
                merged.append(ln)
                continue
            r = good_map.get(m.group(1))
            # 取不到原画时保留原线路兜底，而不是直接删掉，避免频道消失
            merged.append(r["url"] if r else ln)
        os.makedirs(os.path.dirname(os.path.abspath(args.merge)), exist_ok=True)
        with open(args.merge, "w", encoding="utf-8") as f:
            f.write("\n".join(merged) + "\n")
        print("已写出   :", args.merge, "(完整合并版，共 %d 行)" % len(merged))

    print("耗时     : %.1fs" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
