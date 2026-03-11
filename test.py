import akshare as ak

# 获取动力煤期货实时行情
# ZC0 代表动力煤主力合约，通常对应的就是 5500 大卡标准
# df = ak.futures_zh_spot(symbol="ZC0")
# print(df)



stock_hot_tweet_xq_df = ak.stock_hot_tweet_xq(symbol="本周新增")
# 2. 将 DataFrame 转换为原生 Python 列表（每行是一个字典）
# to_dict("records") 会把每行数据转为字典，整体是列表
stock_list = stock_hot_tweet_xq_df.to_dict("records")

# 3. 手动筛选前10条（核心：列表切片）
top10_stocks = stock_list[:10]  # 列表切片，取前10个元素

print(top10_stocks)
