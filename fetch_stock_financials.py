#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A 股财报三表 + 股东户数 + 员工数（2026-11-09 落地）
====================================================
写入 ``stock_financials``（一行 = 一只股票 × 一个报告期，三表合并）、
``stock_holder_counts``、``stock_employee_snapshots``，并创建
``v_stock_financials_quarterly`` 视图（单季差分 + 派生指标，**不落派生表**）。

数据源（**东方财富免费公开接口，无需 key / 无需登录 / 不碰 PDF**）
------------------------------------------------------------------
1) 财报三表   GET https://datacenter.eastmoney.com/securities/api/data/v1/get
              source=HSF10&client=PC&columns=ALL
   reportName = ``RPT_F10_FINANCE_{G|B|S|I}{BALANCE|INCOME|CASHFLOW}``
     G=通用（319/203/254 字段） B=银行 S=证券 I=保险
   ⚠ **G-INCOME 覆盖全部四类**（ORG_TYPE 会标银行/证券/保险），所以利润表只拉 G；
     但 **G-BALANCE / G-CASHFLOW 对金融股返回空**，金融股必须走 B/S/I。
     ⇒ 每个报告期 9 个任务：INCOME×1 + BALANCE×4 + CASHFLOW×4。
   ⚠ ``pageSize`` **硬上限 500**（传 1000/10000 仍只返 500），必须翻页。
   ⚠ 全市场按报告期返回混着 .BJ / .NQ / A25xxx 拟上市主体（2015 年报 count
     高达 19082），**必须按 `stocks` 名录 5562 只过滤**后再写库。
   批量 ``filter=(REPORT_DATE='2026-09-30')`` 与单只
   ``filter=(SECUCODE="600519.SH")`` 数值逐字段一致（已实测）。

2) 股东户数   GET https://datacenter-web.eastmoney.com/api/data/v1/get
              reportName=RPT_HOLDERNUM_DET, source=WEB&client=WEB
   ``END_DATE`` 是**统计截止日**，不必等于季末（茅台 64 期里 02 月 6 期、04 月 1 期）。
   按 (code, end_date) 唯一，映射季度时取该季内最后一个 END_DATE，不硬凑季末。
   2016 起全市场约 40.97 万行 / 820 页（含非目标主体，过滤后约 25 万）。
   ⚠ ``HOLDER_NUM_RATIO`` 是**百分比数值**（-6.0863 = −6.09%），不是小数比例；
     新股上市首期 ``PRE_HOLDER_NUM`` 基数极小（个位数），涨幅可达 1.6e8（%）→
     列必须 numeric(24,4)，用 numeric(12,4) 会在 34 万行处 overflow（2026-11-09 踩过）。

3) 员工数     GET 同 1)，reportName=RPT_F10_BASIC_ORGINFO（EMP_NUM）
   ⚠ **只有当前快照**，字段里没有 REPORT_DATE / UPDATE_DATE，7 个按报告期端点候选全废。
     季报本来就不披露员工数 ⇒ 一年存一条快照，逐年累积，别宣称「逐季度」。
   ⚠ 该报表**不能带 sortColumns=REPORT_DATE**（报「排序列不存在」），分页不带 sort。

口径（决定数字对不对）
----------------------
* 三表是**年初至今累计值**，不是单季。利润表/现金流表单季由视图 LAG 年内差分，
  且要求上一期 quarter_num 恰好 = 当前−1，缺季返回 NULL（不用 0 充数）。
  **资产负债表是季末时点值，绝不差分**。
* 毛利率分母 = ``operate_income``（营业收入），**不是** total_operate_income。
  茅台 2026Q3：89.581094% = 东财 XSMLL 89.581093798；用营业总收入会得 89.761896%。
  速动比 (流动资产−存货)/流动负债 = 5.5505 = 东财 SD 5.550459。
  **金融业毛利率 / 速动比不适用**，视图里直接置 NULL。
* notice_date / update_date 取三张表里**最晚**的（PG GREATEST）。
  ⚠ 源侧追溯更正全年发生（2026-11-09 当天 252 行 UPDATE，含 2024 年报），
  所以增量按 ``UPDATE_DATE`` 回看，而不是「只在披露季跑」。
* raw = {"BALANCE":{...},"INCOME":{...},"CASHFLOW":{...}}，只保留**有值**的源侧字段；
  金额单位元、CURRENCY=CNY。``--no-raw`` 可关掉以省磁盘。
* 报告期默认从 **2016-01-01**（2026-11-09 用户拍板「近 10 年」）。源侧 1990 年报 /
  1998 现金流 / 2002 季报起更早，需要时 ``--full --since 1990-01-01``。
  ⚠ 2002 年前源侧几乎没有季报，属源侧缺口，不是抓漏。

增量语义
--------
* 默认（不带 --full）＝**增量**，走三块：
    财报    ``(UPDATE_DATE>='今天-14天')(REPORT_DATE>='2016-01-01')``
            + ``(NOTICE_DATE>='今天-14天')`` 兜底新披露，9 个端点各拉一遍；
            ⚠ B/S/I 模板常返回「返回数据为空」（**0 行，不是失败**），照常跳过。
    股东户数 ``(HOLD_NOTICE_DATE>='今天-14天')``。
    员工数   全市场拉一次，当年已有快照则 DO NOTHING。
  全程幂等 UPSERT，约 1~2 分钟。
* ``--full``＝首次全量：逐报告期两阶段并发翻页（每期 9 个端点），
  **一个报告期的 9 个任务全部成功且写库后才写进度**，失败不记进度、下次自动补。
  进度文件 ``.workbuddy/stock_financials_progress.json``。
* 二次跑 ``--full`` 会跳过已完成报告期；增量复跑应为 0 行新增（UPSERT 覆盖同值）。

用法
----
    python fetch_stock_financials.py --status                  # 只看库内状态
    python fetch_stock_financials.py --full                   # 首次全量（后台跑，可续跑）
    python fetch_stock_financials.py --full --since 2020-01-01
    python fetch_stock_financials.py                         # 增量（周频挂这个）
    python fetch_stock_financials.py --dry-run --full --limit-periods 1
    python fetch_stock_financials.py --full --codes 600519,000001   # 联调
    python fetch_stock_financials.py --full --force-holders    # 只补股东户数全量（跳过已完成报告期）
    python fetch_stock_financials.py --backfill                # 从 raw 回填 other_income（加列后跑一次）
    python fetch_stock_financials.py --status
"""

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json, execute_values

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://localhost/globaldata")
BASE_DIR = Path(__file__).parent
PROGRESS = BASE_DIR / ".workbuddy" / "stock_financials_progress.json"

SEC = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
WEB = "https://datacenter-web.eastmoney.com/api/data/v1/get"
UA = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"),
    "Referer": "https://data.eastmoney.com/",
}

DEFAULT_SINCE = "2016-01-01"     # 用户拍板「近 10 年」
PAGE_SIZE = 500                  # 东财硬上限
TIMEOUT = 25
WORKERS = 12
INCREMENTAL_DAYS = 14
SOURCE = "eastmoney_datacenter_RPT_F10_FINANCE+RPT_HOLDERNUM_DET+RPT_F10_BASIC_ORGINFO"

# 每个报告期的 9 个任务：利润表 G 全覆盖；资产负债 / 现金流要四套模板补齐金融股
TASKS = (
    [("INCOME", "G")]
    + [("BALANCE", t) for t in "GBSI"]
    + [("CASHFLOW", t) for t in "GBSI"]
)
TEMPLATE_NAME = {"G": "通用", "B": "银行", "S": "证券", "I": "保险"}
SRC_COL = {"INCOME": "inc_source", "BALANCE": "bal_source", "CASHFLOW": "cf_source"}

# db 列 ← 源侧字段。字段名已用 600519 2026Q3 原始报文逐个核对（见探针）。
FIELDS = {
    "INCOME": [
        ("total_operate_income", "TOTAL_OPERATE_INCOME"),
        ("operate_income", "OPERATE_INCOME"),
        ("operate_cost", "OPERATE_COST"),
        ("total_operate_cost", "TOTAL_OPERATE_COST"),
        ("operate_profit", "OPERATE_PROFIT"),
        ("total_profit", "TOTAL_PROFIT"),
        ("parent_netprofit", "PARENT_NETPROFIT"),
        ("netprofit", "NETPROFIT"),
        ("income_tax", "INCOME_TAX"),
        ("sale_expense", "SALE_EXPENSE"),
        ("manage_expense", "MANAGE_EXPENSE"),
        ("finance_expense", "FINANCE_EXPENSE"),
        ("research_expense", "RESEARCH_EXPENSE"),
        # 「其他收益」不是费用，但和费用同一段位置（研发费用之后），
        # 2026-11-09 追加：季度对比表要用单季值。加列前跑过全量的老行需 --backfill。
        ("other_income", "OTHER_INCOME"),
        ("basic_eps", "BASIC_EPS"),
        ("diluted_eps", "DILUTED_EPS"),
    ],
    "BALANCE": [
        ("total_assets", "TOTAL_ASSETS"),
        ("total_current_assets", "TOTAL_CURRENT_ASSETS"),
        ("total_liabilities", "TOTAL_LIABILITIES"),
        ("total_current_liab", "TOTAL_CURRENT_LIAB"),
        ("total_equity", "TOTAL_EQUITY"),
        ("total_parent_equity", "TOTAL_PARENT_EQUITY"),
        ("minority_equity", "MINORITY_EQUITY"),
        ("monetaryfunds", "MONETARYFUNDS"),
        ("accounts_rece", "ACCOUNTS_RECE"),
        ("inventory", "INVENTORY"),
        ("fixed_asset", "FIXED_ASSET"),
        ("cip", "CIP"),
        ("goodwill", "GOODWILL"),
        ("short_loan", "SHORT_LOAN"),
        ("long_loan", "LONG_LOAN"),
        ("accounts_payable", "ACCOUNTS_PAYABLE"),
    ],
    "CASHFLOW": [
        ("netcash_operate", "NETCASH_OPERATE"),
        ("netcash_invest", "NETCASH_INVEST"),
        ("netcash_finance", "NETCASH_FINANCE"),
        ("sales_services", "SALES_SERVICES"),
        ("pay_staff_cash", "PAY_STAFF_CASH"),
        ("construct_long_asset", "CONSTRUCT_LONG_ASSET"),
        ("cce_add", "CCE_ADD"),
        ("begin_cce", "BEGIN_CCE"),
        ("end_cce", "END_CCE"),
    ],
}

COMMON_COLS = ["code", "report_date", "report_year", "quarter_num",
               "report_type", "org_type", "notice_date", "update_date", "currency"]

# ─────────────────────────────────────────────────────────────────────────────
# DDL：本仓库无 migrations，幂等 CREATE / ALTER；视图用 CREATE OR REPLACE
# ─────────────────────────────────────────────────────────────────────────────
SQL_ENSURE = """
CREATE TABLE IF NOT EXISTS stock_financials (
    code                    varchar(10) NOT NULL,
    report_date             date NOT NULL,
    report_year             integer NOT NULL,
    quarter_num             integer NOT NULL,
    report_type             varchar(12),
    org_type                varchar(12),
    notice_date             date,
    update_date             date,
    currency                varchar(8),
    total_operate_income    numeric(24,2),
    operate_income          numeric(24,2),
    operate_cost            numeric(24,2),
    total_operate_cost      numeric(24,2),
    operate_profit          numeric(24,2),
    total_profit            numeric(24,2),
    parent_netprofit        numeric(24,2),
    netprofit               numeric(24,2),
    income_tax              numeric(24,2),
    sale_expense            numeric(24,2),
    manage_expense          numeric(24,2),
    finance_expense         numeric(24,2),
    research_expense        numeric(24,2),
    other_income            numeric(24,2),
    basic_eps               numeric(16,4),
    diluted_eps             numeric(16,4),
    total_assets            numeric(24,2),
    total_current_assets    numeric(24,2),
    total_liabilities       numeric(24,2),
    total_current_liab      numeric(24,2),
    total_equity            numeric(24,2),
    total_parent_equity     numeric(24,2),
    minority_equity         numeric(24,2),
    monetaryfunds           numeric(24,2),
    accounts_rece           numeric(24,2),
    inventory               numeric(24,2),
    fixed_asset             numeric(24,2),
    cip                     numeric(24,2),
    goodwill                numeric(24,2),
    short_loan              numeric(24,2),
    long_loan               numeric(24,2),
    accounts_payable        numeric(24,2),
    netcash_operate         numeric(24,2),
    netcash_invest          numeric(24,2),
    netcash_finance         numeric(24,2),
    sales_services          numeric(24,2),
    pay_staff_cash          numeric(24,2),
    construct_long_asset    numeric(24,2),
    cce_add                 numeric(24,2),
    begin_cce               numeric(24,2),
    end_cce                 numeric(24,2),
    inc_source              varchar(40),
    bal_source              varchar(40),
    cf_source               varchar(40),
    raw                     jsonb,
    source                  varchar(200),
    updated_at              timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_financials_date   ON stock_financials(report_date);
CREATE INDEX IF NOT EXISTS idx_stock_financials_update ON stock_financials(update_date);
-- ⚠ 幂等加列：2026-11-09 之前建的表没有 other_income（当时只从 raw 读「其他收益」）。
--   加列后必须跑一次 `python fetch_stock_financials.py --backfill`，
--   否则历史行该列为 NULL → 单季差分整列显示「—」。
ALTER TABLE stock_financials ADD COLUMN IF NOT EXISTS other_income numeric(24,2);

CREATE TABLE IF NOT EXISTS stock_holder_counts (
    code              varchar(10) NOT NULL,
    end_date          date NOT NULL,
    holder_num        bigint,
    pre_holder_num    bigint,
    holder_num_change bigint,
    holder_num_ratio  numeric(24,4),   -- ⚠ 百分比数值(-6.0863 = -6.09%)，新股首期可达 1e8
    avg_hold_num      numeric(20,4),
    avg_market_cap    numeric(24,2),
    total_market_cap  numeric(24,2),
    total_a_shares    numeric(24,2),
    notice_date       date,
    change_reason     text,
    source            varchar(200),
    updated_at        timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, end_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_holder_counts_notice ON stock_holder_counts(notice_date);
-- ⚠ 幂等改宽：老库按 numeric(12,4) 建过，新股上市首期涨幅可达 1.6e8% 装不下。
--   重复执行无副作用，已有数据不丢（只是放宽精度）。
ALTER TABLE stock_holder_counts ALTER COLUMN holder_num_ratio TYPE numeric(24,4);

CREATE TABLE IF NOT EXISTS stock_employee_snapshots (
    code            varchar(10) NOT NULL,
    snapshot_year   integer NOT NULL,
    emp_num         integer,
    collected_at    date,
    source          varchar(200),
    updated_at      timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (code, snapshot_year)
);
"""


def sql_view() -> str:
    """单季差分视图。LAG 只在同年内、且上一期季度号恰好差 1 时才做减法。

    ⚠ 用 DROP + CREATE 而不是 CREATE OR REPLACE：视图体是 `SELECT q.*`，
      而 q 是 `f.* + 若干 LAG 列` —— 新加的 LAG 落在 f.* 之后、派生列之前，
      `q.*` 展开的列位置会变，PG 会报「不能把视图字段名从 q_parent_netprofit
      改成 prev_sale」。新增列只能追加在视图末尾，所以整段重建。
      视图没有依赖对象、也没有 GRANT/COMMENT，DROP+CREATE 在同一事务内原子生效。
    """
    return """
DROP VIEW IF EXISTS v_stock_financials_quarterly;
CREATE VIEW v_stock_financials_quarterly AS
WITH q AS (
    SELECT f.*,
           LAG(quarter_num)          OVER w AS prev_q,
           LAG(parent_netprofit)     OVER w AS prev_np,
           LAG(total_operate_income) OVER w AS prev_rev,
           LAG(operate_income)       OVER w AS prev_oi,
           LAG(operate_cost)         OVER w AS prev_cost,
           LAG(netcash_operate)      OVER w AS prev_cfo,
           -- 季度对比表用的单季科目（2026-11-09 追加）
           LAG(sale_expense)         OVER w AS prev_sale,
           LAG(manage_expense)       OVER w AS prev_manage,
           LAG(research_expense)     OVER w AS prev_research,
           LAG(operate_profit)       OVER w AS prev_opprofit,
           LAG(income_tax)           OVER w AS prev_tax,
           LAG(netprofit)            OVER w AS prev_netprofit,
           -- 2026-11-09 追加第二轮：财务费用 / 其他收益（列在研发费用之后）
           LAG(finance_expense)      OVER w AS prev_finance,
           LAG(other_income)         OVER w AS prev_other_income
    FROM stock_financials f
    WINDOW w AS (PARTITION BY code, report_year ORDER BY quarter_num)
)
SELECT q.*,
       CASE WHEN quarter_num = 1 THEN parent_netprofit
            WHEN prev_q = quarter_num - 1 THEN parent_netprofit - prev_np
            ELSE NULL END AS q_parent_netprofit,
       CASE WHEN quarter_num = 1 THEN total_operate_income
            WHEN prev_q = quarter_num - 1 THEN total_operate_income - prev_rev
            ELSE NULL END AS q_revenue,
       CASE WHEN quarter_num = 1 THEN operate_income
            WHEN prev_q = quarter_num - 1 THEN operate_income - prev_oi
            ELSE NULL END AS q_operate_income,
       CASE WHEN quarter_num = 1 THEN operate_cost
            WHEN prev_q = quarter_num - 1 THEN operate_cost - prev_cost
            ELSE NULL END AS q_operate_cost,
       CASE WHEN quarter_num = 1 THEN netcash_operate
            WHEN prev_q = quarter_num - 1 THEN netcash_operate - prev_cfo
            ELSE NULL END AS q_netcash_operate,
       CASE WHEN org_type <> '通用' THEN NULL
            WHEN operate_income > 0 THEN (operate_income - operate_cost) / operate_income * 100
            ELSE NULL END AS gross_margin,
       CASE WHEN org_type <> '通用' THEN NULL
            WHEN total_operate_income > 0
              THEN parent_netprofit / total_operate_income * 100
            ELSE NULL END AS net_margin,
       CASE WHEN org_type <> '通用' THEN NULL
            WHEN total_current_liab > 0
              THEN (total_current_assets - COALESCE(inventory, 0)) / total_current_liab
            ELSE NULL END AS quick_ratio,
       CASE WHEN org_type <> '通用' THEN NULL
            WHEN total_current_liab > 0
              THEN total_current_assets / total_current_liab
            ELSE NULL END AS current_ratio,
       CASE WHEN total_assets > 0 THEN total_liabilities / total_assets * 100
            ELSE NULL END AS debt_ratio,
       -- ── 2026-11-09 追加：季度对比表用到的单季科目 ──────────────────────
       -- 一律「年内差分」：Q1 直接取累计值；只有 prev_q 恰好 = 当前-1 才相减，
       -- 缺上一期 / 跨年首期无 base 时返回 NULL，**绝不用 0 充数**。
       -- ⚠ 必须追加在已有列之后：CREATE OR REPLACE VIEW 不允许改变原有列的顺序。
       CASE WHEN quarter_num = 1 THEN sale_expense
            WHEN prev_q = quarter_num - 1 THEN sale_expense - prev_sale
            ELSE NULL END AS q_sale_expense,
       CASE WHEN quarter_num = 1 THEN manage_expense
            WHEN prev_q = quarter_num - 1 THEN manage_expense - prev_manage
            ELSE NULL END AS q_manage_expense,
       CASE WHEN quarter_num = 1 THEN research_expense
            WHEN prev_q = quarter_num - 1 THEN research_expense - prev_research
            ELSE NULL END AS q_research_expense,
       CASE WHEN quarter_num = 1 THEN operate_profit
            WHEN prev_q = quarter_num - 1 THEN operate_profit - prev_opprofit
            ELSE NULL END AS q_operate_profit,
       CASE WHEN quarter_num = 1 THEN income_tax
            WHEN prev_q = quarter_num - 1 THEN income_tax - prev_tax
            ELSE NULL END AS q_income_tax,
       CASE WHEN quarter_num = 1 THEN netprofit
            WHEN prev_q = quarter_num - 1 THEN netprofit - prev_netprofit
            ELSE NULL END AS q_netprofit,
       -- 单季毛利润 = 单季营业收入 − 单季营业成本（分母是 operate_income，不是营业总收入）
       CASE WHEN quarter_num = 1 THEN operate_income - operate_cost
            WHEN prev_q = quarter_num - 1
              THEN (operate_income - operate_cost) - (prev_oi - prev_cost)
            ELSE NULL END AS q_gross_profit,
       -- 财务费用 / 其他收益（2026-11-09 追加，只能追加在视图末尾）
       -- 两者都可能为负：财务费用为负 = 利息收入大于支出；其他收益为负 = 净损失。
       CASE WHEN quarter_num = 1 THEN finance_expense
            WHEN prev_q = quarter_num - 1 THEN finance_expense - prev_finance
            ELSE NULL END AS q_finance_expense,
       CASE WHEN quarter_num = 1 THEN other_income
            WHEN prev_q = quarter_num - 1 THEN other_income - prev_other_income
            ELSE NULL END AS q_other_income
FROM q;
"""


def make_upsert(stmt: str) -> tuple[str, list[str]]:
    cols = (COMMON_COLS + [c for c, _ in FIELDS[stmt]]
            + [SRC_COL[stmt], "raw", "source"])
    sets = ["report_year = EXCLUDED.report_year",
            "quarter_num = EXCLUDED.quarter_num",
            "report_type = EXCLUDED.report_type",
            "org_type = EXCLUDED.org_type",
            "currency = EXCLUDED.currency",
            # 三张表各自披露 / 修订时间不同，取最晚的那个
            "notice_date = GREATEST(stock_financials.notice_date, EXCLUDED.notice_date)",
            "update_date = GREATEST(stock_financials.update_date, EXCLUDED.update_date)"]
    sets += [f"{c} = EXCLUDED.{c}" for c, _ in FIELDS[stmt]]
    sets += [f"{SRC_COL[stmt]} = EXCLUDED.{SRC_COL[stmt]}",
             # 三表分开写，按键合并 raw，绝不覆盖其它两张表
             "raw = COALESCE(stock_financials.raw, '{}'::jsonb) || EXCLUDED.raw",
             "source = EXCLUDED.source",
             "updated_at = NOW()"]
    sql = (f"INSERT INTO stock_financials ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT (code, report_date) DO UPDATE SET " + ", ".join(sets))
    return sql, cols


UPSERT_HOLDERS = """
INSERT INTO stock_holder_counts (
    code, end_date, holder_num, pre_holder_num, holder_num_change,
    holder_num_ratio, avg_hold_num, avg_market_cap, total_market_cap,
    total_a_shares, notice_date, change_reason, source)
VALUES %s
ON CONFLICT (code, end_date) DO UPDATE SET
    holder_num        = EXCLUDED.holder_num,
    pre_holder_num    = EXCLUDED.pre_holder_num,
    holder_num_change = EXCLUDED.holder_num_change,
    holder_num_ratio  = EXCLUDED.holder_num_ratio,
    avg_hold_num      = EXCLUDED.avg_hold_num,
    avg_market_cap    = EXCLUDED.avg_market_cap,
    total_market_cap  = EXCLUDED.total_market_cap,
    total_a_shares    = EXCLUDED.total_a_shares,
    notice_date       = EXCLUDED.notice_date,
    change_reason     = EXCLUDED.change_reason,
    source            = EXCLUDED.source,
    updated_at        = NOW()
"""

UPSERT_EMPLOYEES = """
INSERT INTO stock_employee_snapshots (code, snapshot_year, emp_num, collected_at, source)
VALUES %s
ON CONFLICT (code, snapshot_year) DO NOTHING
"""

UPSERT_EMPLOYEES_FORCE = UPSERT_EMPLOYEES.replace(
    "DO NOTHING",
    "DO UPDATE SET emp_num = EXCLUDED.emp_num, collected_at = EXCLUDED.collected_at, "
    "source = EXCLUDED.source, updated_at = NOW()",
)

# ─────────────────────────────────────────────────────────────────────────────
_tls = threading.local()


def session() -> requests.Session:
    if not hasattr(_tls, "s"):
        s = requests.Session()
        s.headers.update(UA)
        _tls.s = s
    return _tls.s


def num(v):
    if v is None or v == "-" or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x else x


def day(v):
    if not v or v == "-":
        return None
    s = str(v)[:10]
    return s if len(s) == 10 and s[4] == "-" else None


def code_of(rec: dict) -> str:
    return str(rec.get("SECURITY_CODE") or "").strip()


def quarter_of(d: str) -> int:
    return {"03-31": 1, "06-30": 2, "09-30": 3, "12-31": 4}.get(d[5:], 0)


def report_name(stmt: str, tmpl: str) -> str:
    return f"RPT_F10_FINANCE_{tmpl}{stmt}"


def dc_page(url: str, rname: str, filt: str, page: int, *, sort: str | None = None,
            st: str = "1", web: bool = False) -> dict:
    """打一页。'返回数据为空' 是**正常 0 行**（B/S/I 模板增量时天天遇到），不是失败。"""
    p = {"reportName": rname, "columns": "ALL", "filter": filt,
         "pageNumber": page, "pageSize": PAGE_SIZE,
         "source": "WEB" if web else "HSF10", "client": "WEB" if web else "PC"}
    if sort:
        p["sortColumns"], p["sortTypes"] = sort, st
    last = None
    for i in range(3):
        try:
            r = session().get(url, params=p, timeout=TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if j.get("success") is False:
                msg = str(j.get("message") or "")
                # 结构性「无此数据」= 0 行，不是失败：B/S/I 模板增量时天天返回
                # 「返回数据为空」，个别模板没有 UPDATE_DATE 列会报「列不存在」。
                # 真正的 filter 写错（「参数有误」）必须抛出来，不能吞。
                if any(k in msg for k in ("返回数据为空", "报表配置不存在",
                                          "列不存在", "排序列不存在")):
                    return {"count": 0, "pages": 0, "data": []}
                raise RuntimeError(f"源侧失败: {msg[:80]}")
            res = j.get("result") or {}
            return {"count": res.get("count") or 0,
                    "pages": res.get("pages") or 0,
                    "data": res.get("data") or []}
        except Exception as exc:                       # noqa: BLE001
            last = exc
            if i < 2:
                time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"{type(last).__name__}: {str(last)[:100]}")


def dc_all(url: str, rname: str, filt: str, *, sort: str | None = None,
           web: bool = False, pool: ThreadPoolExecutor | None = None,
           max_pages: int = 0) -> list[dict]:
    """两阶段并发：先拿 pages，再把剩余页并发拉完。"""
    first = dc_page(url, rname, filt, 1, sort=sort, web=web)
    rows = list(first["data"])
    pages = min(first["pages"] or 1, max_pages) if max_pages else (first["pages"] or 1)
    if pages <= 1:
        return rows
    jobs = [(url, rname, filt, p, sort, web) for p in range(2, pages + 1)]
    if pool is None:
        for u, rn, fl, p, so, w in jobs:
            rows.extend(dc_page(u, rn, fl, p, sort=so, web=w)["data"])
    else:
        futs = {pool.submit(dc_page, u, rn, fl, p, sort=so, web=w): p
                for u, rn, fl, p, so, w in jobs}
        for f in as_completed(futs):
            rows.extend(f.result()["data"])   # 任一页失败 → 抛，整期不记进度
    return rows


# ─────────────────────────────────────────────────────────────────────────────
def load_codes(conn, only: str = "") -> set[str]:
    if only:
        return {c.strip() for c in only.split(",") if c.strip()}
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM stocks")
        return {r[0] for r in cur.fetchall()}


def build_stmt_rows(stmt: str, records: list[dict], codes: set[str],
                    with_raw: bool, since: str) -> tuple[list[tuple], int, int]:
    """按 (code, report_date) 去重（金融模板优先），过滤名录，构造 UPSERT 行。"""
    by_key: dict[tuple[str, str], dict] = {}
    dropped = 0
    for rec in records:
        code = code_of(rec)
        if code not in codes:
            dropped += 1
            continue
        rd = day(rec.get("REPORT_DATE"))
        if not rd or rd < since or quarter_of(rd) == 0:
            dropped += 1
            continue
        key = (code, rd)
        old = by_key.get(key)
        # 同一 (code, 报告期) 若竟被多个模板返回（实测不会发生），金融模板优先
        if old and (old.get("ORG_TYPE") or "通用") != "通用" and (rec.get("ORG_TYPE") or "通用") == "通用":
            continue
        by_key[key] = rec

    rname_key = stmt
    vals: list[tuple] = []
    for (code, rd), rec in by_key.items():
        q = quarter_of(rd)
        common = {
            "code": code,
            "report_date": rd,
            "report_year": int(rd[:4]),
            "quarter_num": q,
            "report_type": rec.get("REPORT_TYPE"),
            "org_type": rec.get("ORG_TYPE"),
            "notice_date": day(rec.get("NOTICE_DATE")),
            "update_date": day(rec.get("UPDATE_DATE")),
            "currency": rec.get("CURRENCY"),
        }
        row = [common[c] for c in COMMON_COLS]
        for _, src in FIELDS[stmt]:
            row.append(num(rec.get(src)))
        row.append(report_name(stmt, rec.get("_TEMPLATE") or "G"))
        raw = ({rname_key: {k: v for k, v in rec.items()
                            if v is not None and v != "" and not k.startswith("_")}}
               if with_raw else {})
        row.append(Json(raw))
        row.append(SOURCE)
        vals.append(tuple(row))
    return vals, len(by_key), dropped


def write_financials(conn, per_stmt: dict[str, list[dict]], codes: set[str],
                     with_raw: bool, since: str) -> dict:
    """三表分三次 UPSERT（只更新自己的列），返回统计。"""
    stats = {}
    for stmt in ("INCOME", "BALANCE", "CASHFLOW"):
        sql, _ = make_upsert(stmt)
        vals, kept, dropped = build_stmt_rows(stmt, per_stmt.get(stmt, []), codes, with_raw, since)
        stats[stmt] = {"rows": len(vals), "kept": kept, "dropped": dropped}
        if vals:
            with conn.cursor() as cur:
                execute_values(cur, sql, vals, page_size=500)
    return stats


def build_holder_rows(records: list[dict], codes: set[str], since: str) -> list[tuple]:
    out, seen = [], set()
    for r in records:
        code = str(r.get("SECURITY_CODE") or "").strip()
        ed = day(r.get("END_DATE"))
        if code not in codes or not ed or ed < since or (code, ed) in seen:
            continue
        seen.add((code, ed))
        out.append((
            code, ed, r.get("HOLDER_NUM"), r.get("PRE_HOLDER_NUM"),
            r.get("HOLDER_NUM_CHANGE"), num(r.get("HOLDER_NUM_RATIO")),
            num(r.get("AVG_HOLD_NUM")), num(r.get("AVG_MARKET_CAP")),
            num(r.get("TOTAL_MARKET_CAP")), num(r.get("TOTAL_A_SHARES")),
            day(r.get("HOLD_NOTICE_DATE")), r.get("CHANGE_REASON"), SOURCE,
        ))
    return out


def build_employee_rows(records: list[dict], codes: set[str], year: int) -> list[tuple]:
    out, seen = [], set()
    for r in records:
        code = str(r.get("SECURITY_CODE") or "").strip()
        n = r.get("EMP_NUM")
        if code not in codes or n in (None, "", "-") or code in seen:
            continue
        seen.add(code)
        out.append((code, year, int(n), date.today().isoformat(), SOURCE))
    return out


# ─────────────────────────────────────────────────────────────────────────────
def period_list(since: str, today: date) -> list[str]:
    out = []
    for y in range(int(since[:4]), today.year + 1):
        for md in ("03-31", "06-30", "09-30", "12-31"):
            d = f"{y}-{md}"
            if d >= since and date.fromisoformat(d) <= today:
                out.append(d)
    return out


def load_progress() -> dict:
    if PROGRESS.exists():
        try:
            d = json.loads(PROGRESS.read_text(encoding="utf-8"))
            if isinstance(d, dict):
                return d
        except Exception:                              # noqa: BLE001
            pass
    return {"periods": [], "holders_full": None, "employees_year": None}


def save_progress(p: dict) -> None:
    PROGRESS.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS.write_text(json.dumps(p, ensure_ascii=False, indent=1), encoding="utf-8")


def fetch_period(rd: str, pool: ThreadPoolExecutor) -> dict[str, list[dict]]:
    """一个报告期的 9 个端点：并发拿首页 → 展开剩余页 → 并发拉完。"""
    filt = f"(REPORT_DATE='{rd}')"
    jobs = [(stmt, tmpl, report_name(stmt, tmpl)) for stmt, tmpl in TASKS]
    first = {}
    futs = {pool.submit(dc_page, SEC, rn, filt, 1, sort="SECURITY_CODE", st="1"): (stmt, tmpl, rn)
            for stmt, tmpl, rn in jobs}
    for f in as_completed(futs):
        stmt, tmpl, rn = futs[f]
        first[(stmt, tmpl)] = (rn, f.result())

    more = {}
    for (stmt, tmpl), (rn, res) in first.items():
        pages = res["pages"] or 1
        for p in range(2, pages + 1):
            more[pool.submit(dc_page, SEC, rn, filt, p, sort="SECURITY_CODE", st="1")] = (stmt, tmpl)

    collected: dict[str, list[dict]] = {s: [] for s in ("INCOME", "BALANCE", "CASHFLOW")}
    for (stmt, tmpl), (_, res) in first.items():
        for rec in res["data"]:
            rec["_TEMPLATE"] = tmpl
            collected[stmt].append(rec)
    for f in as_completed(more):
        stmt, tmpl = more[f]
        for rec in f.result()["data"]:
            rec["_TEMPLATE"] = tmpl
            collected[stmt].append(rec)
    return collected


def run_full(conn, args, codes: set[str], today: date) -> int:
    periods = [p for p in period_list(args.since, today)
               if p not in set(load_progress()["periods"])]
    if args.limit_periods:
        periods = periods[:args.limit_periods]
    print(f"[full] 待抓报告期 {len(periods)} 个（{periods[0] if periods else '-'} ~ "
          f"{periods[-1] if periods else '-'}），已跳过 {len(load_progress()['periods'])} 个", flush=True)
    if not periods and not args.dry_run:
        print("[full] 报告期全部已完成", flush=True)

    prog = load_progress()
    done = set(prog["periods"])
    failed = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for idx, rd in enumerate(periods, 1):
            try:
                per_stmt = fetch_period(rd, pool)
                n = sum(len(v) for v in per_stmt.values())
                if args.dry_run:
                    print(f"  [{idx}/{len(periods)}] {rd}: {n} 行（dry-run 不写库）", flush=True)
                    if idx == 1:
                        for s, v in per_stmt.items():
                            if v:
                                print(f"      {s} 样例: {code_of(v[0])} "
                                      f"{TEMPLATE_NAME.get(v[0].get('_TEMPLATE'),'?')} "
                                      f"{day(v[0].get('REPORT_DATE'))}")
                    continue
                with conn:
                    stats = write_financials(conn, per_stmt, codes, not args.no_raw, args.since)
                # ⚠ 联调参数（限量 / 指定股票）下绝不记进度，否则正式全量会跳过这些期
                if not (args.limit_periods or args.limit_pages or args.codes):
                    done.add(rd)
                    prog["periods"] = sorted(done)
                    save_progress(prog)
                print(f"  [{idx}/{len(periods)}] {rd}: 源侧 {n} 行 → 入库 "
                      f"利润 {stats['INCOME']['rows']} / 资产负债 {stats['BALANCE']['rows']} / "
                      f"现金流 {stats['CASHFLOW']['rows']}；过滤 {sum(v['dropped'] for v in stats.values())}；"
                      f"{time.time()-t0:.0f}s", flush=True)
            except Exception as exc:                    # noqa: BLE001
                failed.append(rd)
                print(f"  [WARN] {rd} 失败（不计进度，下次自动补）: "
                      f"{type(exc).__name__}: {str(exc)[:120]}", flush=True)

    if args.dry_run:
        print(f"\n[dry-run] 完成，未写库，总耗时 {time.time()-t0:.1f}s")
        return 0

    # 股东户数全量（只在首次 / 未完成时跑），员工数每次 full 都刷一份当年快照
    if not args.skip_holders and (args.force_holders or not prog.get("holders_full")):
        try:
            with ThreadPoolExecutor(max_workers=args.workers) as hp:
                n = run_holders(conn, args, codes, full=True, today=today, pool=hp)
            if not (args.limit_periods or args.limit_pages or args.codes):
                prog["holders_full"] = today.isoformat()
                save_progress(prog)
            print(f"[full] 股东户数全量入库 {n} 行", flush=True)
        except Exception as exc:                        # noqa: BLE001
            failed.append("holders")
            print(f"[WARN] 股东户数全量失败（不计进度）: {exc}", flush=True)
    if not args.skip_employees:
        try:
            run_employees(conn, args, codes, today, force=False)
        except Exception as exc:                        # noqa: BLE001
            failed.append("employees")
            print(f"[WARN] 员工数快照失败: {exc}", flush=True)

    print(f"[full] 完成，耗时 {time.time()-t0:.1f}s；失败任务 {len(failed)} 个"
          + (f" → {failed}" if failed else ""), flush=True)
    return 2 if failed else 0


def run_incremental(conn, args, codes: set[str], today: date) -> int:
    t0 = time.time()
    lo = (today - timedelta(days=args.incremental_days)).isoformat()
    per_stmt: dict[str, list[dict]] = {s: [] for s in ("INCOME", "BALANCE", "CASHFLOW")}
    errors = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        jobs = []
        for stmt, tmpl in TASKS:
            rn = report_name(stmt, tmpl)
            for col in ("UPDATE_DATE", "NOTICE_DATE"):
                flt = f"({col}>='{lo}')(REPORT_DATE>='{args.since}')"
                jobs.append((stmt, tmpl, rn, flt, col))
        futs = {pool.submit(dc_all, SEC, rn, flt, sort="SECURITY_CODE", web=False, pool=None):
                (stmt, tmpl, col) for stmt, tmpl, rn, flt, col in jobs}
        for f in as_completed(futs):
            stmt, tmpl, col = futs[f]
            try:
                for rec in f.result():
                    rec["_TEMPLATE"] = tmpl
                    per_stmt[stmt].append(rec)
            except Exception as exc:                    # noqa: BLE001
                errors.append(f"{report_name(stmt, tmpl)}/{col}: {type(exc).__name__}")
                print(f"  [WARN] {report_name(stmt, tmpl)} {col} 失败: {str(exc)[:100]}", flush=True)
    print(f"[增量] 财报源侧 {sum(len(v) for v in per_stmt.values())} 行 "
          f"（UPDATE_DATE/NOTICE_DATE >= {lo}），失败 {len(errors)} 组", flush=True)
    if not args.dry_run:
        with conn:
            stats = write_financials(conn, per_stmt, codes, not args.no_raw, args.since)
        print("  入库 " + " / ".join(f"{s} {v['rows']}" for s, v in stats.items()), flush=True)
    if not args.skip_holders:
        try:
            n = run_holders(conn, args, codes, full=False, today=today, pool=None)
            print(f"[增量] 股东户数 {n} 行", flush=True)
        except Exception as exc:                        # noqa: BLE001
            errors.append("holders")
            print(f"  [WARN] 股东户数失败: {exc}", flush=True)
    if not args.skip_employees:
        try:
            run_employees(conn, args, codes, today, force=args.force_employees)
        except Exception as exc:                        # noqa: BLE001
            errors.append("employees")
            print(f"  [WARN] 员工数失败: {exc}", flush=True)
    print(f"[增量] 完成，耗时 {time.time()-t0:.1f}s", flush=True)
    return 2 if errors else 0


def run_holders(conn, args, codes: set[str], *, full: bool, today: date,
                pool: ThreadPoolExecutor | None) -> int:
    if full:
        flt = f"(END_DATE>='{args.since}')(END_DATE<='{today.isoformat()}')"
    else:
        lo = (today - timedelta(days=args.incremental_days)).isoformat()
        flt = f"(HOLD_NOTICE_DATE>='{lo}')(END_DATE>='{args.since}')"
    rname = "RPT_HOLDERNUM_DET"
    # 全量 2016 起源侧约 41 万行 / 820 页 —— 一次全装内存会到 ~800MB，
    # 所以按 100 页一批「拉完就写库」，中途失败已写批次幂等，不丢一致性。
    first = dc_page(WEB, rname, flt, 1, sort="END_DATE", web=True)
    pages = first["pages"] or 1
    if args.limit_pages:
        pages = min(pages, args.limit_pages)
    total, raw_rows = 0, 0

    def flush(rows: list[dict]) -> int:
        nonlocal total, raw_rows
        raw_rows += len(rows)
        vals = build_holder_rows(rows, codes, args.since)
        if vals and not args.dry_run:
            # ⚠ 必须 with conn 提交：psycopg2 默认 autocommit=False，
            #   只 `with conn.cursor()` 会在连接关闭时静默 rollback（2026-11-09 实测踩过：
            #   日志打印「入库 1475 行」，--status 却是 0 行）。每批提交，中途失败不丢已写批次。
            with conn:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT_HOLDERS, vals, page_size=500)
        total += len(vals)
        return len(vals)

    flush(first["data"])
    for start in range(2, pages + 1, 100):
        batch = list(range(start, min(start + 100, pages + 1)))
        chunk: list[dict] = []
        if pool is None:
            for p in batch:
                chunk.extend(dc_page(WEB, rname, flt, p, sort="END_DATE", web=True)["data"])
        else:
            futs = [pool.submit(dc_page, WEB, rname, flt, p, sort="END_DATE", web=True)
                    for p in batch]
            for f in as_completed(futs):
                chunk.extend(f.result()["data"])   # 任一页失败 → 抛，进度不记
        flush(chunk)
        if full:
            print(f"    股东户数 {min(start + 99, pages)}/{pages} 页，累计处理 {total} 行", flush=True)
    if args.dry_run:
        print(f"  [dry-run] 股东户数源侧 {raw_rows} 行 → 过滤后 {total} 行，未写库")
    return total


def run_employees(conn, args, codes: set[str], today: date, *, force: bool) -> int:
    # ⚠ RPT_F10_BASIC_ORGINFO 不能带 sortColumns=REPORT_DATE；不带 sort 直接翻页
    rows = dc_all(SEC, "RPT_F10_BASIC_ORGINFO", '(SECURITY_TYPE_CODE="058001001")',
                  sort=None, web=False, pool=None, max_pages=args.limit_pages or 0)
    vals = build_employee_rows(rows, codes, today.year)
    if args.dry_run:
        print(f"  [dry-run] 员工数源侧 {len(rows)} 行 → 过滤后 {len(vals)} 行，未写库")
        return len(vals)
    if vals:
        with conn:                      # 同上：不提交就会被 rollback
            with conn.cursor() as cur:
                execute_values(cur, UPSERT_EMPLOYEES_FORCE if force else UPSERT_EMPLOYEES,
                               vals, page_size=500)
    print(f"  员工数快照 {len(vals)} 只（{today.year} 年，"
          f"{'覆盖' if force else '已有则跳过'}）", flush=True)
    return len(vals)


def backfill_other_income(conn, dry_run: bool = False) -> int:
    """从 raw 回填历史行的 other_income。

    ⚠ 背景：other_income 是 2026-11-09 才加的列（在此之前「其他收益」只存在 raw 里）。
      加列的 DDL 对老行只写 NULL，而增量按 UPDATE_DATE 回看，**永远不会重抓这些老行**，
      所以必须单独回填一次，否则季度对比表的「其他收益」整列是「—」。

    只回填 raw 里确实有 OTHER_INCOME 且是纯数字的行：raw 为 NULL（--no-raw 跑过）
    或源侧没披露的行保持 NULL —— 单季差分显示「—」，不用 0 充数。
    幂等：只动 other_income IS NULL 的行，重复跑得到 0 行。
    """
    cond = r"""other_income IS NULL
            AND (raw->'INCOME'->>'OTHER_INCOME') ~ '^-?[0-9]+(\.[0-9]+)?$'"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*)::int FROM stock_financials WHERE {cond}")
        pending = cur.fetchone()[0]
    if dry_run:
        print(f"[backfill] other_income 待回填 {pending} 行（dry-run 不写库）", flush=True)
        return pending
    with conn:                          # 不 with conn 会被静默 rollback（本项目踩过）
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE stock_financials
                   SET other_income = (raw->'INCOME'->>'OTHER_INCOME')::numeric
                 WHERE {cond}
            """)
            n = cur.rowcount
    print(f"[backfill] other_income 回填 {n} 行（待回填 {pending} 行）", flush=True)
    return n


def show_status(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT code) AS stocks,
                   MIN(report_date)::text AS earliest, MAX(report_date)::text AS latest,
                   MAX(update_date)::text AS last_update,
                   COUNT(parent_netprofit) AS with_np,
                   COUNT(total_assets) AS with_assets,
                   COUNT(netcash_operate) AS with_cfo,
                   COUNT(other_income) AS with_other,
                   pg_size_pretty(pg_total_relation_size('stock_financials')) AS size
            FROM stock_financials
        """)
        f = cur.fetchone()
        cur.execute("""SELECT COUNT(*) AS rows, COUNT(DISTINCT code) AS stocks,
                              MIN(end_date)::text AS earliest, MAX(end_date)::text AS latest
                       FROM stock_holder_counts""")
        h = cur.fetchone()
        cur.execute("""SELECT COUNT(*) AS rows, COUNT(DISTINCT code) AS stocks,
                              MIN(snapshot_year)::text AS earliest,
                              MAX(snapshot_year)::text AS latest, SUM(emp_num) AS sum_emp
                       FROM stock_employee_snapshots""")
        e = cur.fetchone()
    print("stock_financials:")
    print(f"  {f[0]} 行 / {f[1]} 只 / {f[2]} ~ {f[3]} / 源侧最后修订 {f[4]} / 表大小 {f[9]}")
    print(f"  有归母净利 {f[5]} / 有总资产 {f[6]} / 有经营现金流 {f[7]} / "
          f"有其他收益 {f[8]}（老行需 --backfill 回填）")
    print("stock_holder_counts:")
    print(f"  {h[0]} 行 / {h[1]} 只 / {h[2]} ~ {h[3]}")
    print("stock_employee_snapshots:")
    print(f"  {e[0]} 行 / {e[1]} 只 / {e[2]} ~ {e[3]} 年 / 员工合计 {e[4]}")


def main() -> int:
    ap = argparse.ArgumentParser(description="A 股财报三表 + 股东户数 + 员工数")
    ap.add_argument("--full", action="store_true", help="首次全量（默认从 2016-01-01，可续跑）")
    ap.add_argument("--since", default=DEFAULT_SINCE, help=f"报告期起点，默认 {DEFAULT_SINCE}")
    ap.add_argument("--dry-run", action="store_true", help="只抓不写库")
    ap.add_argument("--status", action="store_true", help="只打印库内状态")
    ap.add_argument("--backfill", action="store_true",
                    help="从 raw 回填 other_income（2026-11-09 加列后跑一次，幂等）")
    ap.add_argument("--workers", type=int, default=WORKERS)
    ap.add_argument("--incremental-days", type=int, default=INCREMENTAL_DAYS)
    ap.add_argument("--limit-periods", type=int, default=0, help="全量只跑前 N 个报告期（联调）")
    ap.add_argument("--limit-pages", type=int, default=0, help="股东户数/员工数只翻前 N 页（联调）")
    ap.add_argument("--codes", default="", help="只写指定股票，逗号分隔（联调）")
    ap.add_argument("--no-raw", action="store_true", help="不写 raw jsonb（省磁盘）")
    ap.add_argument("--skip-holders", action="store_true")
    ap.add_argument("--skip-employees", action="store_true")
    ap.add_argument("--force-employees", action="store_true", help="覆盖当年已有员工快照")
    ap.add_argument("--force-holders", action="store_true",
                    help="忽略 holders_full 进度，重跑股东户数全量（补数据 / 修事务后用）")
    args = ap.parse_args()

    if args.dry_run and args.status:
        print("--dry-run 与 --status 不能同用")
        return 1

    conn = psycopg2.connect(DATABASE_URL)
    try:
        if args.status:
            show_status(conn)
            return 0
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_ENSURE)
                cur.execute(sql_view())
        if args.dry_run:
            print("[dry-run] 已确保 DDL / 视图存在；只抓不写业务数据")
        if args.backfill:
            backfill_other_income(conn, args.dry_run)
            return 0
        codes = load_codes(conn, args.codes)
        print(f"名录 {len(codes)} 只（过滤基准）", flush=True)
        today = date.today()
        if args.full:
            return run_full(conn, args, codes, today)
        return run_incremental(conn, args, codes, today)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
