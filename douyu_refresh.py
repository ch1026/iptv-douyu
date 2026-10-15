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
  --extra-rooms  额外收录的房间（源 m3u 里没有的），格式：
                 "5908256=蓝翔新三国,7459925=蓝翔老三国" 或 "5908256,7459925"
                 额外房间不受 --min-width 限制（它本来就是为了把被主仓库过滤掉的
                 低画质房间捞回来），会在报告里标成「额外收录」。
  --extra-rooms-file  从文件读额外房间，一行一个：房间号[=频道名]
  --keep-upstream  合并版里，斗鱼线路换原画直链的同时【保留】上游转流线作为备用。
                 默认开启。上游转流线（live.ottiptv.cc/douyu/xxxx）虽然只有 720P，
                 但它可以重复请求 —— 斗鱼原画直链是「一次性授权」（见下方说明），
                 播放器若先测速再播放，原画线会被测速用掉而播放失败，此时上游线可兜底。
                 用 --no-keep-upstream 关闭。
  --cookie    可选，斗鱼登录 Cookie。带上后多档位房间有机会拿到 rate=0 的真原画
              （不带 Cookie 时斗鱼会把多档位房间降级成 _4000 / _550 转码流）。
  --worker-base  可选。填入你部署好的中转服务地址，例如
                 https://douyu-xxx.yyy.workers.dev
                 填了之后，输出的线路地址会变成 "${worker-base}/d/<房间号>"，
                 由中转服务在每次请求时【重新签发】一条新直链并 302 过去。
                 这是目前唯一能彻底解决「直链一次性、播放器测速后播放失败」的方案，
                 见同目录 douyu-worker.js 与说明文档。
  --relay-rooms  可选，逗号分隔的房间号。这些房间改走 "${worker-base}/r/<房间号>"
                 流式中继，中转服务会顺带做 FLV 时间戳归一化。
                 用于「上游流时间戳损坏」的房间：这类流的 AVC 配置帧带着一个过期
                 时间基准（实测 蓝翔新三国 rid=5908256 的配置帧 ts=16690、首个图像帧
                 ts=327583370，相差 91 小时），播放器时间轴崩溃，表现是
                 「频道加载得出来但播不出」。走 /r/ 后时间戳被重排成 0/40/80…，可正常起播。
                 脚本每次运行都会自动探测并打印哪些房间有此问题。
                 代价：该房间流量过一遍中转服务，首屏比 302 模式略慢，所以只按需开。
  --no-verify 跳过真流校验。默认会抓 FLV 头实测分辨率，因为斗鱼档位名存在虚标
              （实测有标注 1080P、实际只推 720P 的房间）
  --referer   写入斗鱼线路的 Referer，默认 https://www.douyu.com/。
              必须保留：斗鱼部分 CDN（境外 IP 签发时被调度到的 ws3.douyucdn.cn）
              不带 Referer 时返回 HTTP 200 但空流 —— 实测 20 条里只有 2 条能出数据，
              带上 Referer 后 20/20 全部正常。播放器表现就是"频道加载出来但黑屏"。
  --no-referer 不写 Referer 指令（仅当播放器不识别该指令时才用）
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

# 全局可选 Cookie（仅 --cookie 时非空）。拉流探测也带上，避免登录态房间
# 在「签发时是原画、拉流时被判未登录」的情况下误判。
_COOKIE = ""


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


def stream_bytes(url: str, referer=None, secs: float = 2.5,
                 ua: str = "Lavf/58.76.100") -> int:
    """连接后持续读取 secs 秒，返回实际读到的字节数（0 = 空流/失败）。

    用来区分「HTTP 200 但无数据」与「真的能出流」——斗鱼部分 CDN 对无 Referer 的
    请求会返回 200 + video/x-flv 但立刻 EOF，播放器表现就是"连上了播不出来"。
    """
    h = {"User-Agent": ua}
    if referer:
        h["Referer"] = referer
    if _COOKIE:
        h["Cookie"] = _COOKIE
    try:
        resp = http(url, headers=h, timeout=12)
    except Exception:  # noqa: BLE001
        return 0
    t0 = time.time()
    n = 0
    try:
        while time.time() - t0 < secs:
            c = resp.read(65536)
            if not c:
                break
            n += len(c)
    except Exception:  # noqa: BLE001
        pass
    finally:
        try:
            resp.close()
        except Exception:  # noqa: BLE001
            pass
    return n


def probe_referer(urls, tries: int = 5) -> str:
    """抽样统计「带 / 不带 Referer」的出数据率，仅用于日志说明。

    实测数据（20 条串行，ws3.douyucdn.cn）：
        无 Referer  2/20 (10%)   ← 抽到个别特殊流时误判，样本必须够大
        带 Referer 20/20 (100%)
    而不需要 Referer 的 hw3.douyucdn2.cn 带上同样正常，所以策略是**总是附带**，
    不靠探测结果决定是否加头。
    """
    n = min(tries, len(urls))
    if n == 0:
        return "无样本"
    no_ok = yes_ok = 0
    for u in urls[:n]:
        if stream_bytes(u, None, 2.0) > 0:
            no_ok += 1
        if stream_bytes(u, "https://www.douyu.com/", 2.0) > 0:
            yes_ok += 1
    return "无 Referer %d/%d 出数据，带 Referer %d/%d 出数据" % (no_ok, n, yes_ok, n)


def flv_resolution(url: str, timeout: int = 20, referer=None):
    """实抓 FLV 头，返回真实 (宽, 高, 视频码率kbps, 时间戳异常ms)。

    必要性：斗鱼返回的档位名存在虚标（实测有标注 1080P、实际只推 720P 的房间），
    只信档位名会把低清源混进结果，所以这里做一次真流校验。

    第 4 个返回值是「时间戳损坏量」：斗鱼部分转码流的 AVC sequence header 带着
    一个过期时间基准（实测 蓝翔新三国 的 config 帧 ts=16690、首个图像帧 ts=327583370，
    相差 91 小时），播放器时间轴会崩，表现是「加载得出来但播不出」。
    正常流该值为 0；返回值明显偏离 0 就说明这条流需要走中转的时间戳归一化。
    """
    hdr_in = {"User-Agent": "Lavf/58.76.100"}
    if referer:
        hdr_in["Referer"] = referer
    if _COOKIE:
        hdr_in["Cookie"] = _COOKIE
    try:
        resp = http(url, headers=hdr_in, timeout=timeout)
        data = resp.read(260000)
    except Exception:  # noqa: BLE001
        return 0, 0, 0, 0

    def num_after(tag: bytes):
        i = data.find(tag)
        while i >= 0:
            j = i + len(tag)
            if data[j:j + 1] == b"\x00" and j + 9 <= len(data):
                return struct.unpack(">d", data[j + 1:j + 9])[0]
            i = data.find(tag, i + 1)
        return None

    w = num_after(b"width")
    ht = num_after(b"height")
    vk = num_after(b"videodatarate")

    # 顺带扫一遍 tag，算 config 帧与首个图像帧的时间戳差
    ts_gap = 0
    try:
        pos = 13  # FLV header(9) + prevTagSize0(4)
        cfg_ts = None
        while pos + 15 <= len(data):
            ttype = data[pos]
            dsize = int.from_bytes(data[pos + 1:pos + 4], "big")
            ts = int.from_bytes(data[pos + 4:pos + 7], "big") | (data[pos + 7] << 24)
            total = 11 + dsize + 4
            if pos + total > len(data):
                break
            if ttype == 9 and dsize >= 2:
                is_cfg = (data[pos + 11] >> 4) == 1 and data[pos + 12] == 0
                if is_cfg:
                    if cfg_ts is None:
                        cfg_ts = ts
                else:
                    if cfg_ts is None:
                        cfg_ts = ts          # 流里没有 config 帧，以首帧为基准
                    else:
                        gap = ts - cfg_ts
                        if gap < 0 or gap > 5000:
                            ts_gap = gap
                        break
            pos += total
    except Exception:  # noqa: BLE001
        pass

    return (int(w) if w else 0, int(ht) if ht else 0, int(vk) if vk else 0, ts_gap)


class Douyu:
    """斗鱼取流（2024 后新版签名：密钥包 + 迭代 MD5）"""

    KEY_URL = "https://www.douyu.com/wgapi/livenc/liveweb/websec/getEncryption?did=" + DID
    PLAY_URL = "https://www.douyu.com/lapi/live/getH5PlayV1/%s"

    def __init__(self, cdn: str = "hw-h5", cookie: str = ""):
        self.cdn = cdn
        self.cookie = (cookie or "").strip()
        self._kd = None
        self._lock = threading.Lock()

    def _ck(self, h: dict) -> dict:
        if self.cookie:
            h["Cookie"] = self.cookie
        return h

    def key(self, force: bool = False):
        with self._lock:
            if self._kd is not None and not force:
                return self._kd
            last = None
            for _ in range(4):
                try:
                    raw = http(self.KEY_URL, timeout=20,
                               headers=self._ck({"Referer": "https://www.douyu.com/"})
                               ).read().decode("utf-8", "ignore")
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
                    headers=self._ck({
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Referer": "https://www.douyu.com/%s" % rid}),
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
                live = d.get("rtmp_live") or ""
                url = (d.get("rtmp_url") or "").rstrip("/") + "/" + live
                mr = d.get("multirates") or []
                rate_name = (mr[0].get("name") if mr else "") or ""
                bit = (mr[0].get("bit") if mr else 0) or 0
                w, h = parse_resolution(rate_name)
                # 斗鱼对多档位房间 + 未登录请求会降级成转码流，流名形如
                # `9999rMfVyoBTjV0x_4000.flv`（_4000 = 4000kbps 转码）。
                # 无 `_NNN` 后缀才是主播真实推流的原画。实测 24 个热门房间：
                # 多档位房间 17/17 被降级，单/双档位房间 7/7 拿到真原画。
                tc = re.search(r"_(\d+)\.flv$", live.split("?")[0])
                return {"error": 0, "msg": "", "url": url, "rate_name": rate_name,
                        "bit": bit, "width": w, "height": h,
                        "stream": live.split("?")[0],
                        "transcoded": bool(tc),
                        "transcoded_bit": int(tc.group(1)) if tc else 0}
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
    ap.add_argument("--referer", default="https://www.douyu.com/",
                    help="写入斗鱼线路的 Referer，默认 https://www.douyu.com/。"
                         "斗鱼部分 CDN（如 ws3，境外 IP 签发时常被调度到此）不带 Referer 时"
                         "会返回 HTTP 200 + video/x-flv 但立即 EOF，播放器表现为"
                         "「频道加载出来却一直黑屏播不出」")
    ap.add_argument("--no-referer", action="store_true",
                    help="不写入 Referer 指令（仅在播放器不识别该指令时才需要）")
    ap.add_argument("--min-width", type=int, default=1920, help="最小宽度，默认 1920")
    ap.add_argument("--extra-rooms", default="",
                    help='额外收录的房间（源里没有的），如 "5908256=蓝翔新三国,7459925=蓝翔老三国"')
    ap.add_argument("--extra-rooms-file", default=None,
                    help="从文件读额外房间，一行一个：房间号[=频道名]")
    ap.add_argument("--keep-upstream", dest="keep_upstream", action="store_true", default=True,
                    help="合并版保留上游转流线作为备用（默认开启）")
    ap.add_argument("--no-keep-upstream", dest="keep_upstream", action="store_false",
                    help="合并版不保留上游转流线")
    ap.add_argument("--cookie", default="",
                    help="斗鱼登录 Cookie，带上后多档位房间有机会拿到 rate=0 真原画")
    ap.add_argument("--worker-base", default="",
                    help="中转服务地址，填了则输出 ${worker-base}/d/<房间号>，"
                         "由中转服务每次请求重新签发直链（推荐，见 douyu-worker.js）")
    ap.add_argument("--relay-rooms", default="",
                    help="走【流式中继】而非 302 的房间号，逗号分隔，例如 5908256。"
                         "用于斗鱼转码流时间戳损坏的房间（config 帧与首个图像帧差 91 小时，"
                         "播放器加载得出来但播不出）。中继会顺带把时间戳归一化修好；"
                         "代价是流量过一遍中转服务、首屏略慢，所以只给确有需要的房间开。"
                         "仅在配合 --worker-base 时生效。")
    ap.add_argument("--cdn", default="hw-h5", help="斗鱼 CDN 参数，默认 hw-h5")
    ap.add_argument("--limit", type=int, default=0, help="只处理前 N 个房间（调试用）")
    ap.add_argument("--workers", type=int, default=3,
                    help="并发数，默认 3。斗鱼对高频请求会概率性返回空流"
                         "（实测 6 并发时出数据率约 76%%，降到 4 并加重试后更稳），"
                         "调高会更快但更容易被限流误伤")
    ap.add_argument("--min-success-ratio", type=float, default=0.3,
                    help="最低成功率，低于则退出码 2 且不写文件")
    args = ap.parse_args()

    global _COOKIE
    _COOKIE = (args.cookie or "").strip()
    wbase = args.worker_base.rstrip("/")

    t0 = time.time()
    print("源      :", args.src)
    lines = read_source(args.src)
    rooms = collect_rooms(lines)
    n_src = len(rooms)

    # 额外房间：主仓库因为画质门槛过滤掉的房间，在这里手动捞回来
    extra_specs = []
    if args.extra_rooms:
        extra_specs += [s.strip() for s in args.extra_rooms.split(",") if s.strip()]
    if args.extra_rooms_file and os.path.exists(args.extra_rooms_file):
        with open(args.extra_rooms_file, encoding="utf-8", errors="ignore") as f:
            for ln in f:
                ln = ln.split("#")[0].strip()
                if ln:
                    extra_specs.append(ln)

    have = {r["rid"] for r in rooms}
    n_extra = 0
    for spec in extra_specs:
        if "=" in spec:
            rid, nm = spec.split("=", 1)
        else:
            rid, nm = spec, spec
        rid, nm = rid.strip(), nm.strip()
        if not rid.isdigit() or rid in have:
            continue
        have.add(rid)
        rooms.append({"rid": rid, "name": nm, "extinf": None, "extra": True})
        n_extra += 1

    print("斗鱼房间: %d 个（源 %d + 额外 %d）" % (len(rooms), n_src, n_extra))
    if not rooms:
        print("!! 源里没有斗鱼线路，终止（不覆盖已有文件）")
        return 2

    rids = []
    for r in rooms:
        if r["rid"] not in rids:
            rids.append(r["rid"])
    if args.limit:
        rids = rids[:args.limit]
    print("去重房间:", len(rids), "个 | CDN:", args.cdn, "| 并发:", args.workers,
          "| Cookie:", "已提供" if _COOKIE else "无",
          "| 中转:", wbase if args.worker_base else "无")
    print("-" * 78)

    dy = Douyu(cdn=args.cdn, cookie=_COOKIE)
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

    extra_rids = {r["rid"] for r in rooms if r.get("extra")}

    def floor_of(rid):
        """额外房间本就是为了捞回被主仓库按画质过滤掉的频道，所以不设门槛"""
        if rid in extra_rids:
            return 0
        return args.min_width

    ok, low, fail = [], [], []
    for rid in rids:
        r = results[rid]
        if r.get("error"):
            fail.append((rid, r))
        elif HOST_BAD.search(r.get("url", "")):
            fail.append((rid, {"error": "BADHOST", "msg": "被调度到 edgesrv 边缘节点"}))
        elif r.get("width", 0) < floor_of(rid):
            low.append((rid, r))
        else:
            ok.append((rid, r))

    # 斗鱼部分 CDN 需要 Referer 才出流（详见 stream_bytes / probe_referer 注释）。
    # 实测「带 Referer」在两种 CDN 上都是 100% 出数据，所以策略是默认总是附加，
    # 而不是靠探测决定——探测样本小的时候会误判。
    ref_url = None if args.no_referer else (args.referer or None)
    if ref_url and ok:
        print("Referer 探测: %s" % probe_referer([r["url"] for _, r in ok][:5]))
        if args.worker_base:
            print("  -> 走中转服务：/r/ 由服务端补 Referer，/d/ 依赖播放器发 Referer，"
                  "因此 m3u 里仍然写入 #EXTVLCOPT:http-referrer=%s" % ref_url)
        else:
            print("  -> 将为每条斗鱼线路写入 #EXTVLCOPT:http-referrer=%s" % ref_url)

    # 实测校验：斗鱼档位名存在虚标（实测有标注 1080P 但只推 720P 的房间），
    # 只信档位名会把低清源混进结果，所以抓一次真流确认。
    if ok and not args.no_verify:
        print("实测校验: %d 条（读 FLV 头确认真实分辨率）" % len(ok))

        def check_one(item):
            # 斗鱼对高频请求会概率性返回空流（实测：6 并发出数据的约占 76%，
            # 低频请求为 100%），所以探测失败时降速重试一次，避免误判。
            _rid, r = item
            w, h, bit, gap = flv_resolution(r["url"], referer=ref_url)
            if not w:
                time.sleep(1.2)
                w, h, bit, gap = flv_resolution(r["url"], referer=ref_url)
            return w, h, bit, gap

        verified, false_label, ts_bad = [], 0, []
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for (rid, r), (w, h, bit, gap) in zip(ok, ex.map(check_one, ok)):
                if w and w < floor_of(rid):
                    false_label += 1
                    bad = dict(r)
                    bad["verified"] = "档位名 %s 虚标，实测 %dx%d" % (
                        r.get("rate_name") or "?", w, h)
                    low.append((rid, bad))
                    continue
                good = dict(r)
                good["verified"] = ("实测 %dx%d %dkbps" % (w, h, bit)) if w else "未校验成功（探测超时）"
                if gap:
                    # 时间戳损坏：直链形式在多数播放器上无法起播，
                    # 必须走中转的 /r/ 通道做归一化（见 douyu-worker.js）
                    good["ts_gap"] = gap
                    ts_bad.append((rid, gap))
                verified.append((rid, good))
        ok = verified
        print("  校验后达标: %d 条 | 拦截虚标: %d 条" % (len(ok), false_label))
        if ts_bad:
            # 严重：配置帧时间基准与真实媒体帧差了 1 小时以上 ——
            # 以配置帧建立时间轴的播放器会直接崩掉（「加载出来但播不出」）
            hard = [(r, g) for r, g in ts_bad if abs(g) > 3600000]
            soft = [(r, g) for r, g in ts_bad if abs(g) <= 3600000]
            print("  ⚠️ 时间戳异常 %d 条（配置帧与首个媒体帧不在同一时间基准上）:"
                  % len(ts_bad))
            if hard:
                print("     严重 %d 条（偏差 > 1 小时，多数播放器无法起播，建议加进 --relay-rooms）:"
                      % len(hard))
                for rid, gap in sorted(hard, key=lambda x: -abs(x[1])):
                    nm = next((x["name"] for x in rooms if x["rid"] == rid), rid)
                    print("       %-24s rid=%-9s %6.1f 小时" % (nm[:24], rid, gap / 3600000.0))
            if soft:
                print("     轻微 %d 条（秒级偏差，多数播放器可播，但可能拖慢起播）:" % len(soft))
                for rid, gap in sorted(soft, key=lambda x: -abs(x[1]))[:6]:
                    nm = next((x["name"] for x in rooms if x["rid"] == rid), rid)
                    print("       %-24s rid=%-9s %6.1f 秒" % (nm[:24], rid, gap / 1000.0))
                if len(soft) > 6:
                    print("       ... 另有 %d 条" % (len(soft) - 6))

    # ★★★ 关键修复（2026-10-16）★★★
    # 直链是「一次性授权」：同一条 URL 的第二次连接会被 CDN 在读到
    # 0.3~2MB 时掐断。而上面的画质校验（check_one）已经把每条 r["url"]
    # 都打开过一次 —— 如果把这条「二手链接」写给播放器，它的播放就是
    # 第二次连接，表现正是「播两秒就失败」，且所有频道无一幸免。
    # 所以这里为每条达标线路【重新签发】一条全新直链用于输出；
    # 校验用的那条就让它就地消耗。中转模式下输出的是 /d/、/r/ 地址
    # （每次请求实时签发），不存在这个问题，无需重签。
    if ok and not args.worker_base:
        print("重新签发: %d 条（校验消耗的是一次性授权，输出必须用全新直链）" % len(ok))

        def resign_one(item):
            _rid, _r = item
            for attempt in range(3):
                try:
                    nr = dy.play(_rid)
                    if not nr.get("error") and nr.get("url") \
                            and not HOST_BAD.search(nr["url"]):
                        return nr
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(1.0)
            return None

        fresh, failed_resign = [], 0
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for (rid, r), nr in zip(ok, ex.map(resign_one, ok)):
                if nr is None:
                    # 重签失败只能写旧链接 —— 大概率播不出，但聊胜于无
                    failed_resign += 1
                    fresh.append((rid, r))
                    continue
                g = dict(r)
                g["url"] = nr["url"]
                # 以新签发返回的档位/码率为准（同一房间通常一致）
                if nr.get("rate_name"):
                    g["rate_name"] = nr["rate_name"]
                if nr.get("bit"):
                    g["bit"] = nr["bit"]
                if nr.get("width"):
                    g["width"], g["height"] = nr.get("width"), nr.get("height")
                fresh.append((rid, g))
        ok = fresh
        if failed_resign:
            print("  ⚠️ %d 条重签失败，仍写旧链接（可能播不出）" % failed_resign)

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
    # 注意 Actions runner 是 UTC 时区，localtime() 会得到 UTC，
    # 所以这里显式 +8 小时换算成北京时间再写进注释，避免时间戳误导。
    stamp = time.strftime("%Y-%m-%d %H:%M:%S",
                          time.gmtime(time.time() + 8 * 3600)) + " (UTC+8)"
    use_worker = bool(args.worker_base)
    relay_set = set(x.strip() for x in (args.relay_rooms or "").replace(";", ",")
                    .replace(" ", ",").split(",") if x.strip())

    def out_url(rid, r):
        """最终写进 m3u 的线路地址。填了 --worker-base 就指向中转服务，
        由它每次重新签发新直链 —— 这是绕开「直链一次性」的唯一办法。

        位于 --relay-rooms 里的房间走 /r/（流式中继 + FLV 时间戳归一化），
        用来修复斗鱼部分转码流的时间戳损坏：这类流的 config 帧时间戳与
        首个图像帧差了 91 小时，播放器时间轴崩溃，表现是「加载出来但播不出」。"""
        if not use_worker:
            return r["url"]
        mode = "r" if rid in relay_set else "d"
        return "%s/%s/%s" % (wbase, mode, rid)

    # Referer 指令是【两种模式都要写】的：
    #   - 直链模式：斗鱼境外 CDN（ws3）不带 Referer 会「连上但立刻 EOF」；
    #   - 中转模式：/r/ 中继由服务端自己补 Referer，播放器带不带都行（带了无害），
    #     但 /d/ 是 302 —— 播放器【直接】去连斗鱼 CDN，
    #     而中转服务多半在境外、签发的必然是 ws3 族地址，这就需要播放器自己发 Referer。
    # 早期版本在 worker 模式下省掉了这行，属于 bug，已修。
    hdr = "#EXTVLCOPT:http-referrer=%s" % ref_url if ref_url else None

    out = ["#EXTM3U x-tvg-url=\"https://ch1026.github.io/iptv-api/epg.xml\"",
           "# 生成时间: %s" % stamp]
    if use_worker:
        out.append("# 线路由中转服务 %s 动态签发直链（每次请求都是新的 token）" % wbase)
        if relay_set:
            out.append("# 其中 %s 走流式中继模式，并已做 FLV 时间戳归一化"
                       "（这些房间上游流时间戳损坏，直链形式在多数播放器上无法起播）"
                       % ",".join(sorted(relay_set)))
    else:
        out.append("# ⚠️ 直链为「一次性授权」：同一地址只能成功拉流一次，"
                   "播放器若先测速再播放、或播放中断线重连，都会失败。")
        out.append("#    彻底解决请部署中转服务（见 douyu-worker.js），"
                   "并给本脚本加 --worker-base <你的服务地址>。")
    if hdr:
        out.append("# 斗鱼线路需要 Referer，已在每条线路前写入 %s" % hdr)
    local = ["# iptv-api 本地源 / 白名单格式：频道名,直链",
             "# ⚠️ 斗鱼直链为一次性授权且需要 Referer 请求头，本格式无法携带请求头，"
             "建议优先使用 m3u 输出或中转服务"]
    report = [("频道名", "房间号", "状态", "档位名", "流类型", "宽度", "高度",
               "码率kbps", "实测", "时间戳", "直链")]
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
            report.append((name, rid, "额外-剔除" if room.get("extra") else "剔除",
                           reason, "", 0, 0, 0, "", "", ""))
            continue
        if room["extinf"]:
            out.append(room["extinf"])
        else:
            out.append("#EXTINF:-1 group-title=\"斗鱼补充\",%s" % name)
        if hdr:
            out.append(hdr)
        # 时间戳损坏的房间：直链形式在多数播放器上「加载得出来但播不出」，
        # 走中转 /r/ 通道（FLV 时间戳归一化）才能修好。
        gap = r.get("ts_gap") or 0
        if gap and not (use_worker and rid in relay_set):
            out.append("# ⚠️ 本房间上游流时间戳损坏（配置帧跳变 %.1f 小时），"
                       "多数播放器会加载后无法起播；部署中转服务并加 "
                       "--relay-rooms %s 可修复" % (gap / 3600000.0, rid))
        out.append(out_url(rid, r))
        written.add(rid)
        kind = ("转码%sk" % r["transcoded_bit"]) if r.get("transcoded") else "原画"
        report.append((name, rid,
                       "额外OK" if room.get("extra") else "OK",
                       r["rate_name"], kind, r["width"], r["height"], r["bit"],
                       r.get("verified", ""),
                       ("⚠️ 跳变 %.1f 小时" % (gap / 3600000.0)) if gap else "正常",
                       out_url(rid, r)))
        if name not in seen_local:
            seen_local.add(name)
            local.append("%s,%s" % (name, out_url(rid, r)))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print("已写出   :", args.out, "(%d 个频道)" % len(written))

    if args.local:
        with open(args.local, "w", encoding="utf-8") as f:
            f.write("\n".join(local) + "\n")
        print("已写出   :", args.local, "(%d 条)" % len(seen_local))
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
                merged.append("# 斗鱼线路已替换为官方原画直链 | 生成时间: %s" % stamp)
                merged.append("# 注意：斗鱼直链是「一次性授权」——同一地址只能成功拉流一次。")
                merged.append("#      播放器先测速再播放、或播放中重连，都会失败。"
                              "要彻底解决请用 --worker-base 中转。")
                merged.append("#      另有部分房间的上游流时间戳损坏（配置帧与首个媒体帧"
                              "不在同一时间基准），会「加载得出来但播不出」，"
                              "需用 --relay-rooms 走中转修复；具体是哪些见下方逐条标注。")
                if hdr:
                    merged.append("# 斗鱼线路需要 Referer，已在每条斗鱼线路前写入 %s" % hdr)
                noted = True
                continue
            m = DOUYU_LINE.search(ln)
            if not m:
                merged.append(ln)
                continue
            rid = m.group(1)
            r = good_map.get(rid)
            # 取不到原画时保留原线路兜底，而不是直接删掉，避免频道消失
            if not r:
                merged.append(ln)
                continue
            gap = r.get("ts_gap") or 0
            if gap and not (use_worker and rid in relay_set):
                merged.append("# ⚠️ 本线上游流时间戳损坏（配置帧跳变 %.1f 小时），"
                              "多数播放器会加载后无法起播；部署中转服务并加 "
                              "--relay-rooms %s 可修复" % (gap / 3600000.0, rid))
            if hdr:
                merged.append(hdr)
            merged.append(out_url(rid, r))
            # 保留上游转流线作为备用：它可重复请求，是唯一能扛住「播放器测速」的线路
            if args.keep_upstream and not use_worker:
                merged.append(ln)
        # 额外房间（源里本来没有的）追加到末尾，自成一组「斗鱼补充」
        extra_ok = [(r["rid"], r) for r in rooms
                    if r.get("extra") and r["rid"] in good_map]
        if extra_ok:
            merged.append("# 以下为额外收录频道（主仓库因画质门槛过滤掉，此处手动加回）")
            for rid, room in extra_ok:
                r = good_map[rid]
                merged.append("#EXTINF:-1 group-title=\"斗鱼补充\",%s" % room["name"])
                gap = r.get("ts_gap") or 0
                if gap and not (use_worker and rid in relay_set):
                    merged.append("# ⚠️ 本线上游流时间戳损坏（配置帧跳变 %.1f 小时），"
                                  "多数播放器会加载后无法起播；部署中转服务并加 "
                                  "--relay-rooms %s 可修复" % (gap / 3600000.0, rid))
                if hdr:
                    merged.append(hdr)
                merged.append(out_url(rid, r))
        os.makedirs(os.path.dirname(os.path.abspath(args.merge)), exist_ok=True)
        with open(args.merge, "w", encoding="utf-8") as f:
            f.write("\n".join(merged) + "\n")
        print("已写出   :", args.merge, "(完整合并版，共 %d 行)" % len(merged))

    print("耗时     : %.1fs" % (time.time() - t0))
    return 0


if __name__ == "__main__":
    sys.exit(main())
