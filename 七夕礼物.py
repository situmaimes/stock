import asyncio
import datetime
import os
import time
from io import StringIO

import aiohttp
import pandas as pd
import requests

from config import codes_path, send_tos
from mail import send_mail

# from itertools import chain

url = 'http://stock.gtimg.cn/data/index.php'

params = {
    "appn": "detail",
    "action": "download",
    "c": "sz000004",
    "d": "20190806"
}

today = datetime.datetime.now().strftime("%Y%m%d")
today_print = datetime.datetime.now().strftime("%Y-%m-%d")


def now():
    return time.time()


def download(date, all_codes, over):
    # todo：handle aiohttp.client_exceptions.ServerDisconnectedError
    params["d"] = date
    semaphore = asyncio.Semaphore(200)

    async def download_one(code):
        params["c"] = code
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp and resp.status == 200:
                    data = await resp.text(encoding="gbk")
                    return data
                else:
                    print(f"{code} failed")
                    return

    async def process_one(code):
        async with semaphore:
            data = await download_one(code)
            if data and data != "暂无数据":
                df = pd.read_csv(StringIO(data), sep="\t",
                                 names=['time', 'price', 'change', 'volume', 'amount', 'type'],
                                 skiprows=[0])
                if df.shape[0] == 0:
                    print(f"{code} no data")
                    return
                line = df.iloc[-1]
                if line["type"] == "卖盘" and line["volume"] >= over:
                    line.name = code
                    df.to_excel(f"data/{date}/{code}.xls")
                    return line

    tasks = [asyncio.ensure_future(process_one(code)) for code in all_codes]
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(asyncio.wait(tasks))
    codes = [task.result() for task in tasks]
    codes = list(filter(lambda x: x is not None, codes))

    # params["d"] = date
    # codes = []
    # download_start = now()
    # for code in all_codes:
    #     params["c"] = code
    #     res = requests.get(url, params=params)
    #     if res.status_code != 200:
    #         print(f"{code} failed")
    #     res.encoding = "gbk"
    #     df = pd.read_table(StringIO(res.text), names=['time', 'price', 'change', 'volume', 'amount', 'type'],
    #                        skiprows=[0])
    #     if df.shape[0] == 0:
    #         continue
    #     line = df.iloc[-1]
    #     if line["type"] == "卖盘" and line["volume"] >= over:
    #         line.name = code
    #         codes.append(line)
    #         df.to_excel(f"data/{date}/{code}.xls")
    # download_end = now()
    # print(f"下载用时 {download_end - download_start} s")

    return codes


def run(over=None):
    text = ''
    # 创建存储文件夹
    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists("result"):
        os.makedirs("result")

    # 存储情况1~5的code
    column1 = []
    column2 = []
    column3 = []
    column4 = []
    column5 = []
    over = over if over else 1000  # 现手以上

    # 读取待查询股票
    all_data = pd.read_excel(codes_path)
    all_data.columns = ["code", "name"]
    all_data.code = all_data.code.str.replace("SZ", "sz").str.replace("SH", "sh")
    all_data = all_data.set_index(['code'])

    # 筛选 现手卖盘大于over数据下载或读取
    if not os.path.isdir(f"data/{today}"):
        os.makedirs(f"data/{today}")
        download_start = now()
        codes_data = download(date=today, all_codes=all_data.index, over=over)
        download_end = now()
        text += f"下载用时 {download_end - download_start} s\n"
        codes_data.sort(key=lambda x: x["volume"], reverse=True)
        codes = [i.name for i in codes_data]
        print(f"已下载 {today_print} 现手卖盘大于 {over} 的股票共计 {len(codes)} 个")
        filter_codes = all_data.loc[codes]
        filter_codes["volume"] = [i["volume"] for i in codes_data]
    else:
        print("已有下载，正在读取相应目录")
        files = os.listdir(f"data/{today}")
        codes = [i[:-4] for i in files]
        # filter_codes = all_data.loc[codes]

    # 情况1-5筛选
    for code in codes:
        # 读取文件，处理时间
        code_data = pd.read_excel(f"data/{today}/{code}.xls")
        code_data['time'] = pd.to_datetime(code_data['time'], format="%H:%M:%S")
        code_data['time'] = code_data['time'].dt.time

        # 第一种情况 9.35前上万白单
        condition = code_data[
            (code_data["time"] < datetime.time(9, 35)) & (code_data["volume"] >= 10000) & (code_data["type"] == "中性盘")]
        if not condition.empty:
            column1.append(code)
            continue

        # 第二种情况 9.35前连续上千白单,夹单不超5
        condition = code_data[
            (code_data["time"] < datetime.time(9, 35)) & (code_data["volume"] >= 1000) & (code_data["type"] == "中性盘")]
        condition_index = list(condition.index)
        if len(condition_index) >= 2:
            delta = [condition_index[i + 1] - condition_index[i] for i in range(len(condition_index) - 1)]
            if min(delta) <= 6:
                column2.append(code)
                continue

        # 第三种情况 9.35前上千白单，10点前无上千买盘或上千卖盘
        condition1 = code_data[
            (code_data["time"] < datetime.time(9, 35)) & (code_data["volume"] >= 1000) & (code_data["type"] == "中性盘")]
        condition2 = code_data[
            (datetime.time(9, 32) < code_data["time"]) & (code_data["time"] < datetime.time(9, 58)) & (
                    code_data["volume"] >= 1000) & (
                code_data["type"].isin(["买盘", "卖盘"]))]
        if (not condition1.empty) and condition2.empty:
            column3.append(code)
            continue

        # 第四种情况 9.32前连续100~1000间白单，夹不超过5单，全天无上千买盘或上千卖盘
        condition1 = code_data[
            (code_data["time"] < datetime.time(9, 32)) & (code_data["volume"] >= 100) & (code_data["volume"] < 1000) & (
                    code_data["type"] == "中性盘")]
        condition2 = code_data[(code_data["volume"] >= 1000) & (
            code_data["type"].isin(["买盘", "卖盘"])) & (datetime.time(9, 32) < code_data["time"]) & (
                                       code_data["time"] < datetime.time(14, 55))]
        condition1_index = list(condition1.index)
        if condition2.empty:
            if len(condition1_index) >= 2:
                delta = [condition1_index[i + 1] - condition1_index[i] for i in range(len(condition1_index) - 1)]
                if min(delta) <= 6:
                    column4.append(code)
                    continue

        # 第五种情况 9.32前有100~1000间白单，全天无上千买盘或上千卖盘
        if condition2.empty and (not condition1.empty):
            column5.append(code)
            continue

    # 数据汇总写出
    final_data = [column1, column2, column3, column4, column5]
    # condition = [[f"情况{i+1}"] * len(data) for i, data in enumerate(final_data)]
    # condition = list(chain(*condition))
    # all_columns = list(chain(*final_data))
    final_name_data = [[all_data.loc[i]["name"] for i in j] for j in final_data]
    max_len = max(len(item) for item in final_name_data)
    for item in final_name_data:
        while len(item) < max_len:
            item.append(None)

    result = pd.DataFrame({"情况1": final_name_data[0],
                           "情况2": final_name_data[1],
                           "情况3": final_name_data[2],
                           "情况4": final_name_data[3],
                           "情况5": final_name_data[4]})

    text += f"情况 1~5 符合条件数目 {[len(item) for item in final_data]}\n"
    # print(f"结果如下:")
    # print(result)
    result.to_excel(f"result/{today}结果.xls", index=False)

    # final_codes = filter_codes.loc[all_columns]
    # final_codes["condition"] = condition
    # final_codes.sort_values(by="volume", ascending=False, inplace=True)
    # final_codes.to_excel(f"result/{today}排序.xls")

    return text


def is_weekday(date):
    params["d"] = date
    resp = requests.get(url, params=params)
    resp.encoding = "gbk"
    return resp.text != '暂无数据'


def main():
    if is_weekday(today):
        start = now()
        text = run()
        end = now()
        text += f"总用时 {end - start} s\n"
        text = f"今天是 {today_print}\n" + text + "请查收附件."
        send_mail(send_tos=send_tos, name="Simon Yang", subject=f"{today_print}结果", text=text,
                  att_urls=[f"result/{today}结果.xls"])
    else:
        send_mail(send_tos=send_tos, name="Simon Yang", subject=f"{today_print}结果", text="今天休市，无数据")


if __name__ == '__main__':
    main()
