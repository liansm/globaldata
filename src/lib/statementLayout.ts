/**
 * A 股三张表的**科目字典**（字段名 → 中文名 + 顺序 + 层级）。
 *
 * 为什么手写而不是从源侧拿：
 *   东财 `RPT_F10_FINANCE_*` 只返英文字段名（319/203/254 个），**不返中文标签**，
 *   前端包是懒加载分包、抠不到映射表；akshare 的中文表来自新浪、字段名与本库对不上。
 *   所以按标准报表格式手写一遍，并用**雪球同公司同报告期的数值逐条核对**（见
 *   `.workbuddy/verify_statement_layout.py`，2026-11-09 招商轮船 601872 2026Q3：
 *   货币资金 119.24 亿 / 交易性金融资产 43.56 亿 / 流动资产合计 271.81 亿 等 29 项全部一致）。
 *
 * 三段式约定：
 *   - `sections[].title` = 「流动资产」「非流动资产」这类粗体小标题；为 null 不显示标题行
 *   - `kind: 'sub'`   = 「其中：」明细，前端缩进
 *   - `kind: 'total'` = 合计/小计，前端加粗
 *   - `sign: -1`      = 源侧存的是「带符号的收益」，报表习惯显示为正的损失额
 *                      （资产减值损失、信用减值损失实测：源 -489.54 万 ↔ 雪球 +489.54 万）
 *   - `fmt: 'plain'`  = 每股收益这类，不做亿/万换算
 *   - `alt`           = 新准则/旧准则二选一，主键为空时才取 alt
 */
export type Stmt = 'BALANCE' | 'INCOME' | 'CASHFLOW'
export type LineKind = 'item' | 'sub' | 'total'

export interface Line {
  key: string
  alt?: string
  label: string
  kind?: LineKind
  sign?: -1
  fmt?: 'money' | 'plain'
}

export interface Section {
  title: string | null
  lines: Line[]
}

export const STMT_LABEL: Record<Stmt, string> = {
  BALANCE: '资产负债表',
  INCOME: '利润表',
  CASHFLOW: '现金流量表',
}

export const BALANCE_LAYOUT: Section[] = [
  {
    title: '流动资产',
    lines: [
      // ↓ 银行 / 券商 / 保险专有，通用企业为空、前端自动整行隐藏
      { key: 'CASH_DEPOSIT_PBC', label: '现金及存放中央银行款项' },
      { key: 'DEPOSIT_INTERBANK', label: '存放同业款项' },
      { key: 'CUSTOMER_DEPOSIT', label: '客户资金存款' },
      { key: 'CUSTOMER_EXCESS_RESERVE', label: '客户备付金' },
      { key: 'FIN_FUND', label: '融出资金' },
      { key: 'TIME_DEPOSIT', label: '定期存款' },
      { key: 'MONETARYFUNDS', label: '货币资金' },
      { key: 'SETTLE_EXCESS_RESERVE', label: '结算备付金' },
      { key: 'LEND_FUND', label: '拆出资金' },
      { key: 'TRADE_FINASSET_NOTFVTPL', alt: 'FVTPL_FINASSET', label: '交易性金融资产' },
      { key: 'DERIVE_FINASSET', label: '衍生金融资产' },
      { key: 'NOTE_ACCOUNTS_RECE', label: '应收票据及应收账款' },
      { key: 'NOTE_RECE', label: '其中：应收票据', kind: 'sub' },
      { key: 'ACCOUNTS_RECE', label: '应收账款', kind: 'sub' },
      { key: 'RECEIVABLES', label: '应收款项' },
      { key: 'FINANCE_RECE', label: '应收款项融资' },
      { key: 'PREPAYMENT', label: '预付款项' },
      { key: 'PREMIUM_RECE', label: '应收保费' },
      { key: 'REINSURE_RECE', label: '应收分保账款' },
      { key: 'RC_RESERVE_RECE', label: '应收分保合同准备金' },
      { key: 'TOTAL_OTHER_RECE', alt: 'OTHER_RECE', label: '其他应收款' },
      { key: 'INTEREST_RECE', label: '其中：应收利息', kind: 'sub' },
      { key: 'DIVIDEND_RECE', label: '应收股利', kind: 'sub' },
      { key: 'BUY_RESALE_FINASSET', label: '买入返售金融资产' },
      { key: 'INVENTORY', label: '存货' },
      { key: 'CONTRACT_ASSET', label: '合同资产' },
      { key: 'HOLDSALE_ASSET', label: '划分为持有待售的资产' },
      { key: 'NONCURRENT_ASSET_1YEAR', label: '一年内到期的非流动资产' },
      { key: 'OTHER_CURRENT_ASSET', alt: 'CURRENT_ASSET_OTHER', label: '其他流动资产' },
      { key: 'TOTAL_CURRENT_ASSETS', label: '流动资产合计', kind: 'total' },
    ],
  },
  {
    title: '非流动资产',
    lines: [
      { key: 'LOAN_ADVANCE', label: '发放贷款和垫款' },
      { key: 'CREDITOR_INVEST', label: '债权投资' },
      { key: 'OTHER_CREDITOR_INVEST', label: '其他债权投资' },
      { key: 'AVAILABLE_SALE_FINASSET', label: '可供出售金融资产' },
      { key: 'HOLD_MATURITY_INVEST', label: '持有至到期投资' },
      { key: 'LONG_RECE', label: '长期应收款' },
      { key: 'LONG_EQUITY_INVEST', label: '长期股权投资' },
      { key: 'OTHER_EQUITY_INVEST', label: '其他权益工具投资' },
      { key: 'OTHER_NONCURRENT_FINASSET', label: '其他非流动金融资产' },
      { key: 'INVEST_REALESTATE', label: '投资性房地产' },
      { key: 'FIXED_ASSET', label: '固定资产' },
      { key: 'CIP', label: '在建工程' },
      { key: 'PRODUCTIVE_BIOLOGY_ASSET', label: '生产性生物资产' },
      { key: 'OIL_GAS_ASSET', label: '油气资产' },
      { key: 'USERIGHT_ASSET', label: '使用权资产' },
      { key: 'INTANGIBLE_ASSET', label: '无形资产' },
      { key: 'DEVELOP_EXPENSE', label: '开发支出' },
      { key: 'GOODWILL', label: '商誉' },
      { key: 'LONG_PREPAID_EXPENSE', label: '长期待摊费用' },
      { key: 'DEFER_TAX_ASSET', label: '递延所得税资产' },
      { key: 'OTHER_NONCURRENT_ASSET', alt: 'NONCURRENT_ASSET_OTHER', label: '其他非流动资产' },
      // 银行 / 保险专有：放在资产末尾，金融股合并分组后顺序才顺（发放贷款 → 投资 → 其他）
      { key: 'OTHER_ASSET', alt: 'ASSET_OTHER', label: '其他资产' },
      { key: 'IND_ACC_ASSET', label: '独立账户资产' },
      { key: 'TOTAL_NONCURRENT_ASSETS', label: '非流动资产合计', kind: 'total' },
      { key: 'TOTAL_ASSETS', label: '资产总计', kind: 'total' },
    ],
  },
  {
    title: '流动负债',
    lines: [
      { key: 'SHORT_LOAN', label: '短期借款' },
      { key: 'LOAN_PBC', label: '向中央银行借款' },
      { key: 'IOFI_DEPOSIT', label: '同业及其他金融机构存放款项' },
      { key: 'ACCEPT_DEPOSIT', label: '吸收存款' },
      { key: 'ACCEPT_DEPOSIT_INTERBANK', label: '吸收存款及同业存放' },
      { key: 'AGENT_TRADE_SECURITY', label: '代理买卖证券款' },
      { key: 'AGENT_UNDERWRITE_SECURITY', label: '代理承销证券款' },
      { key: 'BORROW_FUND', label: '拆入资金' },
      { key: 'TRADE_FINLIAB_NOTFVTPL', alt: 'FVTPL_FINLIAB', label: '交易性金融负债' },
      { key: 'DERIVE_FINLIAB', label: '衍生金融负债' },
      { key: 'NOTE_ACCOUNTS_PAYABLE', label: '应付票据及应付账款' },
      { key: 'NOTE_PAYABLE', label: '其中：应付票据', kind: 'sub' },
      { key: 'ACCOUNTS_PAYABLE', label: '应付账款', kind: 'sub' },
      { key: 'ADVANCE_RECEIVABLES', label: '预收款项' },
      { key: 'CONTRACT_LIAB', label: '合同负债' },
      { key: 'SELL_REPO_FINASSET', label: '卖出回购金融资产款' },
      { key: 'FEE_COMMISSION_PAYABLE', label: '应付手续费及佣金' },
      { key: 'STAFF_SALARY_PAYABLE', label: '应付职工薪酬' },
      { key: 'TAX_PAYABLE', label: '应交税费' },
      { key: 'TOTAL_OTHER_PAYABLE', alt: 'OTHER_PAYABLE', label: '其他应付款' },
      { key: 'INTEREST_PAYABLE', label: '其中：应付利息', kind: 'sub' },
      { key: 'DIVIDEND_PAYABLE', label: '应付股利', kind: 'sub' },
      { key: 'REINSURE_PAYABLE', label: '应付分保账款' },
      { key: 'ADVANCE_PREMIUM', label: '预收保费' },
      { key: 'COMPENSATE_PAYABLE', label: '应付赔付款' },
      { key: 'POLICY_BONUS_PAYABLE', label: '应付保单红利' },
      { key: 'INSURED_DEPOSIT_INVEST', label: '保户储金及投资款' },
      { key: 'HOLDSALE_LIAB', label: '持有待售负债' },
      { key: 'NONCURRENT_LIAB_1YEAR', label: '一年内到期的非流动负债' },
      { key: 'OTHER_CURRENT_LIAB', alt: 'CURRENT_LIAB_OTHER', label: '其他流动负债' },
      { key: 'TOTAL_CURRENT_LIAB', label: '流动负债合计', kind: 'total' },
    ],
  },
  {
    title: '非流动负债',
    lines: [
      { key: 'INSURANCE_CONTRACT_RESERVE', label: '保险合同准备金' },
      { key: 'LONG_LOAN', label: '长期借款' },
      { key: 'BOND_PAYABLE', label: '应付债券' },
      { key: 'PREFERRED_SHARES_PAYBALE', label: '其中：优先股', kind: 'sub' },
      { key: 'PERPETUAL_BOND_PAYBALE', label: '永续债', kind: 'sub' },
      { key: 'LEASE_LIAB', label: '租赁负债' },
      { key: 'LONG_PAYABLE', label: '长期应付款' },
      { key: 'SHORT_FIN_PAYABLE', label: '短期融资券' },
      { key: 'SUBBOND_PAYABLE', label: '应付次级债' },
      { key: 'LONG_STAFFSALARY_PAYABLE', label: '长期应付职工薪酬' },
      { key: 'PREDICT_LIAB', label: '预计负债' },
      { key: 'DEFER_INCOME', label: '递延收益' },
      { key: 'DEFER_TAX_LIAB', label: '递延所得税负债' },
      { key: 'OTHER_NONCURRENT_LIAB', alt: 'NONCURRENT_LIAB_OTHER', label: '其他非流动负债' },
      { key: 'OTHER_LIAB', alt: 'LIAB_OTHER', label: '其他负债' },
      { key: 'IND_ACC_LIAB', label: '独立账户负债' },
      { key: 'TOTAL_NONCURRENT_LIAB', label: '非流动负债合计', kind: 'total' },
      { key: 'TOTAL_LIABILITIES', label: '负债合计', kind: 'total' },
    ],
  },
  {
    title: '所有者权益',
    lines: [
      { key: 'SHARE_CAPITAL', label: '实收资本（或股本）' },
      { key: 'OTHER_EQUITY_TOOL', label: '其他权益工具' },
      { key: 'PREFERRED_SHARES', label: '其中：优先股', kind: 'sub' },
      { key: 'PERPETUAL_BOND', label: '永续债', kind: 'sub' },
      { key: 'CAPITAL_RESERVE', label: '资本公积' },
      { key: 'TREASURY_SHARES', label: '减：库存股' },
      { key: 'OTHER_COMPRE_INCOME', label: '其他综合收益' },
      { key: 'SPECIAL_RESERVE', label: '专项储备' },
      { key: 'SURPLUS_RESERVE', label: '盈余公积' },
      { key: 'GENERAL_RISK_RESERVE', label: '一般风险准备' },
      { key: 'UNASSIGN_RPOFIT', label: '未分配利润' },
      { key: 'TOTAL_PARENT_EQUITY', label: '归属于母公司所有者权益合计', kind: 'total' },
      { key: 'MINORITY_EQUITY', label: '少数股东权益' },
      { key: 'TOTAL_EQUITY', label: '所有者权益合计', kind: 'total' },
      { key: 'TOTAL_LIAB_EQUITY', label: '负债和所有者权益总计', kind: 'total' },
    ],
  },
]

export const INCOME_LAYOUT: Section[] = [
  {
    title: null,
    lines: [
      { key: 'TOTAL_OPERATE_INCOME', label: '营业总收入', kind: 'total' },
      { key: 'OPERATE_INCOME', label: '其中：营业收入', kind: 'sub' },
      { key: 'OTHER_BUSINESS_INCOME', label: '其他业务收入', kind: 'sub' },
      { key: 'INTEREST_INCOME', label: '利息收入', kind: 'sub' },
      { key: 'EARNED_PREMIUM', label: '已赚保费', kind: 'sub' },
      { key: 'FEE_COMMISSION_INCOME', label: '手续费及佣金收入', kind: 'sub' },
      { key: 'TOTAL_OPERATE_COST', label: '营业总成本', kind: 'total' },
      { key: 'OPERATE_COST', label: '其中：营业成本', kind: 'sub' },
      { key: 'OTHER_BUSINESS_COST', label: '其他业务成本', kind: 'sub' },
      { key: 'INTEREST_EXPENSE', label: '利息支出', kind: 'sub' },
      { key: 'FEE_COMMISSION_EXPENSE', label: '手续费及佣金支出', kind: 'sub' },
      { key: 'SURRENDER_VALUE', label: '退保金', kind: 'sub' },
      { key: 'NET_COMPENSATE_EXPENSE', label: '赔付支出净额', kind: 'sub' },
      { key: 'NET_CONTRACT_RESERVE', label: '提取保险责任准备金净额', kind: 'sub' },
      { key: 'POLICY_BONUS_EXPENSE', label: '保单红利支出', kind: 'sub' },
      { key: 'REINSURE_EXPENSE', label: '分保费用', kind: 'sub' },
      { key: 'OPERATE_TAX_ADD', label: '税金及附加', kind: 'sub' },
      { key: 'SALE_EXPENSE', label: '销售费用', kind: 'sub' },
      { key: 'MANAGE_EXPENSE', label: '管理费用', kind: 'sub' },
      { key: 'RESEARCH_EXPENSE', label: '研发费用', kind: 'sub' },
      { key: 'FINANCE_EXPENSE', label: '财务费用', kind: 'sub' },
      { key: 'FE_INTEREST_EXPENSE', label: '其中：利息费用', kind: 'sub' },
      { key: 'FE_INTEREST_INCOME', label: '利息收入', kind: 'sub' },
      { key: 'OTHER_INCOME', label: '加：其他收益' },
      { key: 'INVEST_INCOME', label: '投资收益' },
      { key: 'INVEST_JOINT_INCOME', label: '其中：对联营企业和合营企业的投资收益', kind: 'sub' },
      { key: 'NET_EXPOSURE_INCOME', label: '净敞口套期收益' },
      { key: 'FAIRVALUE_CHANGE_INCOME', label: '公允价值变动收益' },
      { key: 'CREDIT_IMPAIRMENT_INCOME', alt: 'CREDIT_IMPAIRMENT_LOSS', label: '信用减值损失', sign: -1 },
      { key: 'ASSET_IMPAIRMENT_INCOME', alt: 'ASSET_IMPAIRMENT_LOSS', label: '资产减值损失', sign: -1 },
      { key: 'ASSET_DISPOSAL_INCOME', label: '资产处置收益' },
      { key: 'EXCHANGE_INCOME', label: '汇兑收益' },
      { key: 'OPERATE_PROFIT', label: '营业利润', kind: 'total' },
      { key: 'NONBUSINESS_INCOME', label: '加：营业外收入' },
      { key: 'NONBUSINESS_EXPENSE', label: '减：营业外支出' },
      { key: 'TOTAL_PROFIT', label: '利润总额', kind: 'total' },
      { key: 'INCOME_TAX', label: '减：所得税费用' },
      { key: 'NETPROFIT', label: '净利润', kind: 'total' },
      { key: 'CONTINUED_NETPROFIT', label: '持续经营净利润', kind: 'sub' },
      { key: 'DISCONTINUED_NETPROFIT', label: '终止经营净利润', kind: 'sub' },
      { key: 'PARENT_NETPROFIT', label: '归属于母公司所有者的净利润', kind: 'total' },
      { key: 'MINORITY_INTEREST', label: '少数股东损益' },
      { key: 'DEDUCT_PARENT_NETPROFIT', label: '扣除非经常性损益后的净利润' },
      { key: 'BASIC_EPS', label: '基本每股收益（元/股）', fmt: 'plain' },
      { key: 'DILUTED_EPS', label: '稀释每股收益（元/股）', fmt: 'plain' },
      { key: 'OTHER_COMPRE_INCOME', label: '其他综合收益', kind: 'total' },
      { key: 'PARENT_OCI', label: '归属于母公司所有者的其他综合收益', kind: 'sub' },
      { key: 'UNABLE_OCI', label: '不能重分类进损益的其他综合收益', kind: 'sub' },
      { key: 'ABLE_OCI', label: '将重分类进损益的其他综合收益', kind: 'sub' },
      { key: 'MINORITY_OCI', label: '归属于少数股东的其他综合收益', kind: 'sub' },
      { key: 'TOTAL_COMPRE_INCOME', label: '综合收益总额', kind: 'total' },
      { key: 'PARENT_TCI', label: '归属于母公司所有者的综合收益总额', kind: 'sub' },
      { key: 'MINORITY_TCI', label: '归属于少数股东的综合收益总额', kind: 'sub' },
    ],
  },
]

export const CASHFLOW_LAYOUT: Section[] = [
  {
    title: '经营活动产生的现金流量',
    lines: [
      { key: 'SALES_SERVICES', label: '销售商品、提供劳务收到的现金' },
      { key: 'DEPOSIT_INTERBANK_ADD', label: '客户存款和同业存放款项净增加额' },
      { key: 'LOAN_PBC_ADD', label: '向中央银行借款净增加额' },
      { key: 'OFI_BF_ADD', label: '向其他金融机构拆入资金净增加额' },
      { key: 'RECEIVE_ORIGIC_PREMIUM', label: '收到原保险合同保费取得的现金' },
      { key: 'RECEIVE_REINSURE_NET', label: '收到再保业务现金净额' },
      { key: 'INSURED_INVEST_ADD', label: '保户储金及投资款净增加额' },
      { key: 'DISPOSAL_TFA_ADD', label: '处置交易性金融资产净增加额' },
      { key: 'RECEIVE_INTEREST_COMMISSION', label: '收取利息、手续费及佣金的现金' },
      { key: 'BORROW_FUND_ADD', label: '拆入资金净增加额' },
      { key: 'REPO_BUSINESS_ADD', label: '回购业务资金净增加额' },
      // ↓ 金融业专有，通用企业为空
      { key: 'DEPOSIT_PBC_REDUCE', label: '存放中央银行和同业款项净减少额' },
      { key: 'LEND_FUND_ADD', label: '拆出资金净增加额' },
      { key: 'BUY_RESALE_REDUCE', label: '买入返售金融资产净减少额' },
      { key: 'BORROW_REPO_ADD', label: '卖出回购业务资金净增加额' },
      { key: 'RECEIVE_AGENT_TRADE', label: '代理买卖证券收到的现金净额' },
      { key: 'INSURED_INVEST_REDUCE', label: '保户储金及投资款净减少额' },
      { key: 'RECEIVE_TAX_REFUND', label: '收到的税费返还' },
      { key: 'RECEIVE_OTHER_OPERATE', label: '收到其他与经营活动有关的现金' },
      { key: 'TOTAL_OPERATE_INFLOW', label: '经营活动现金流入小计', kind: 'total' },
      { key: 'BUY_SERVICES', label: '购买商品、接受劳务支付的现金' },
      { key: 'LOAN_ADVANCE_ADD', label: '客户贷款及垫款净增加额' },
      { key: 'PBC_INTERBANK_ADD', label: '存放中央银行和同业款项净增加额' },
      { key: 'PAY_ORIGIC_COMPENSATE', label: '支付原保险合同赔付款项的现金' },
      { key: 'PAY_INTEREST_COMMISSION', label: '支付利息、手续费及佣金的现金' },
      { key: 'PAY_POLICY_BONUS', label: '支付保单红利的现金' },
      { key: 'PAY_STAFF_CASH', label: '支付给职工以及为职工支付的现金' },
      { key: 'PAY_ALL_TAX', label: '支付的各项税费' },
      { key: 'PAY_OTHER_OPERATE', label: '支付其他与经营活动有关的现金' },
      { key: 'TOTAL_OPERATE_OUTFLOW', label: '经营活动现金流出小计', kind: 'total' },
      { key: 'NETCASH_OPERATE', label: '经营活动产生的现金流量净额', kind: 'total' },
    ],
  },
  {
    title: '投资活动产生的现金流量',
    lines: [
      { key: 'WITHDRAW_INVEST', label: '收回投资收到的现金' },
      { key: 'RECEIVE_INVEST_INCOME', label: '取得投资收益收到的现金' },
      { key: 'DISPOSAL_LONG_ASSET', label: '处置固定资产、无形资产和其他长期资产收回的现金净额' },
      { key: 'DISPOSAL_SUBSIDIARY_OTHER', label: '处置子公司及其他营业单位收到的现金净额' },
      { key: 'REDUCE_PLEDGE_TIMEDEPOSITS', label: '减少质押定期存款收回的现金' },
      { key: 'RECEIVE_OTHER_INVEST', label: '收到其他与投资活动有关的现金' },
      { key: 'TOTAL_INVEST_INFLOW', label: '投资活动现金流入小计', kind: 'total' },
      { key: 'CONSTRUCT_LONG_ASSET', label: '购建固定资产、无形资产和其他长期资产支付的现金' },
      { key: 'INVEST_PAY_CASH', label: '投资支付的现金' },
      { key: 'PLEDGE_LOAN_ADD', label: '质押贷款净增加额' },
      { key: 'OBTAIN_SUBSIDIARY_OTHER', label: '取得子公司及其他营业单位支付的现金净额' },
      { key: 'ADD_PLEDGE_TIMEDEPOSITS', label: '增加质押定期存款支付的现金' },
      { key: 'PAY_OTHER_INVEST', label: '支付其他与投资活动有关的现金' },
      { key: 'TOTAL_INVEST_OUTFLOW', label: '投资活动现金流出小计', kind: 'total' },
      { key: 'NETCASH_INVEST', label: '投资活动产生的现金流量净额', kind: 'total' },
    ],
  },
  {
    title: '筹资活动产生的现金流量',
    lines: [
      { key: 'ACCEPT_INVEST_CASH', label: '吸收投资收到的现金' },
      { key: 'SUBSIDIARY_ACCEPT_INVEST', label: '其中：子公司吸收少数股东投资收到的现金', kind: 'sub' },
      { key: 'RECEIVE_LOAN_CASH', label: '取得借款收到的现金' },
      { key: 'ISSUE_BOND', label: '发行债券收到的现金' },
      { key: 'RECEIVE_OTHER_FINANCE', label: '收到其他与筹资活动有关的现金' },
      { key: 'TOTAL_FINANCE_INFLOW', label: '筹资活动现金流入小计', kind: 'total' },
      { key: 'PAY_DEBT_CASH', label: '偿还债务支付的现金' },
      { key: 'ASSIGN_DIVIDEND_PORFIT', label: '分配股利、利润或偿付利息支付的现金' },
      { key: 'SUBSIDIARY_PAY_DIVIDEND', label: '其中：子公司支付给少数股东的股利、利润', kind: 'sub' },
      { key: 'PAY_OTHER_FINANCE', label: '支付其他与筹资活动有关的现金' },
      { key: 'TOTAL_FINANCE_OUTFLOW', label: '筹资活动现金流出小计', kind: 'total' },
      { key: 'NETCASH_FINANCE', label: '筹资活动产生的现金流量净额', kind: 'total' },
    ],
  },
  {
    title: '现金及现金等价物净变动',
    lines: [
      { key: 'RATE_CHANGE_EFFECT', label: '汇率变动对现金及现金等价物的影响' },
      { key: 'CCE_ADD', label: '现金及现金等价物净增加额', kind: 'total' },
      { key: 'BEGIN_CCE', label: '加：期初现金及现金等价物余额' },
      { key: 'END_CCE', label: '期末现金及现金等价物余额', kind: 'total' },
    ],
  },
]

export const LAYOUT: Record<Stmt, Section[]> = {
  BALANCE: BALANCE_LAYOUT,
  INCOME: INCOME_LAYOUT,
  CASHFLOW: CASHFLOW_LAYOUT,
}

export const IS_STMT = (v: string): v is Stmt => v === 'BALANCE' || v === 'INCOME' || v === 'CASHFLOW'
