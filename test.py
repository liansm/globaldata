import akshare as ak
import tushare as ts
from datetime import datetime, timedelta

#stock_hsgt_hist_em_df = ak.stock_hsgt_hist_em(symbol="北向资金")
#print(stock_hsgt_hist_em_df)


#stock_zh_index_daily_em_df = ak.stock_zh_index_daily_em(symbol="sz399812")
#print(stock_zh_index_daily_em_df)

TUSHARE_TOKEN="d15cfba1c586114385df799b70c4809fe82c698c1f7ed1db16dd4afd"
_ts_pro = None

def _get_ts_pro():
    global _ts_pro
    if _ts_pro is None:
        if not TUSHARE_TOKEN:
            raise RuntimeError("TUSHARE_TOKEN not set in environment")
        ts.set_token(TUSHARE_TOKEN)
        _ts_pro = ts.pro_api()
    return _ts_pro


start = (datetime.now() - timedelta(days=365 * 20)).strftime("%Y%m%d")
end   = datetime.now().strftime("%Y%m%d")

pro = _get_ts_pro()
df  = pro.moneyflow_hsgt(start_date=start, end_date=end,
                                  fields="trade_date,north_money,south_money")

for _, row in df.iterrows():
    date_val = row.get("trade_date")
    print(row)