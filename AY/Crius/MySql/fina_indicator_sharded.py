import sys
import time
import datetime
import logging

from sqlalchemy.exc import IntegrityError

import Utils.configuration_file_service as config_service
import Utils.DB_utils as dbUtil

import pandas as pd
import tushare as ts
from sqlalchemy import Column, String, Float, MetaData, Table, create_engine, INT

token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                   property_name=config_service.TS_TOKEN_NAME)
pro = ts.pro_api(token)


def getTableMeta(year: int, metadata: MetaData) -> Table:
    """
    get corresponding table meta data.
    :param year: year of the data
    :return: a Table object representing the table structure
    """
    return Table(
        dbUtil.getTableName(year, "fina_indicator"), metadata,
        Column("ts_code", String(10)),  # 股票代码
        Column("ann_date", String(8)),
        Column("end_date", String(8)),
        Column("eps", Float),  # 基本每股收益
        Column("dt_eps", Float),  # 稀释每股收益
        Column("total_revenue_ps", Float),  # 每股营业总收入
        Column("revenue_ps", Float),  # 每股营业收入
        Column("capital_rese_ps", Float),  # 每股资本公积
        Column("surplus_rese_ps", Float),  # 每股盈余公积
        Column("undist_profit_ps", Float),  # 每股未分配利润
        Column("extra_item", Float),  # 非经常性损益
        Column("profit_dedt", Float),  # 扣除非经常性损益后的净利润
        Column("gross_margin", Float),  # 毛利
        Column("current_ratio", Float),  # 流动比率
        Column("quick_ratio", Float),  # 速动比率
        Column("cash_ratio", Float),  # 保守速动比率
        Column("invturn_days", Float),  # 存货周转天数
        Column("arturn_days", Float),  # 应收账款周转天数
        Column("inv_turn", Float),  # 存货周转率
        Column("ar_turn", Float),  # 应收账款周转率
        Column("ca_turn", Float),  # 流动资产周转率
        Column("fa_turn", Float),  # 固定资产周转率
        Column("assets_turn", Float),  # 总资产周转率
        Column("op_income", Float),  # 经营活动净收益
        Column("valuechange_income", Float),  # 价值变动净收益
        Column("interst_income", Float),  # 利息费用
        Column("daa", Float),  # 折旧与摊销
        Column("ebit", Float),  # 息税前利润
        Column("ebitda", Float),  # 息税折旧摊销前利润
        Column("fcff", Float),  # 企业自由现金流量
        Column("fcfe", Float),  # 股权自由现金流量
        Column("current_exint", Float),  # 无息流动负债
        Column("noncurrent_exint", Float),  # 无息非流动负债
        Column("interestdebt", Float),  # 带息债务
        Column("netdebt", Float),  # 净债务
        Column("tangible_asset", Float),  # 有形资产
        Column("working_capital", Float),  # 营运资金
        Column("networking_capital", Float),  # 营运流动资本
        Column("invest_capital", Float),  # 全部投入资本
        Column("retained_earnings", Float),  # 留存收益
        Column("diluted2_eps", Float),  # 期末摊薄每股收益
        Column("bps", Float),  # 每股净资产
        Column("ocfps", Float),  # 每股经营活动产生的现金流量净额
        Column("retainedps", Float),  # 每股留存收益
        Column("cfps", Float),  # 每股现金流量净额
        Column("ebit_ps", Float),  # 每股息税前利润
        Column("fcff_ps", Float),  # 每股企业自由现金流量
        Column("fcfe_ps", Float),  # 每股股东自由现金流量
        Column("netprofit_margin", Float),  # 销售净利率
        Column("grossprofit_margin", Float),  # 销售毛利率
        Column("cogs_of_sales", Float),  # 销售成本率
        Column("expense_of_sales", Float),  # 销售期间费用率
        Column("profit_to_gr", Float),  # 净利润/营业总收入
        Column("saleexp_to_gr", Float),  # 销售费用/营业总收入
        Column("adminexp_of_gr", Float),  # 管理费用/营业总收入
        Column("finaexp_of_gr", Float),  # 财务费用/营业总收入
        Column("impai_ttm", Float),  # 资产减值损失/营业总收入
        Column("gc_of_gr", Float),  # 营业总成本/营业总收入
        Column("op_of_gr", Float),  # 营业利润/营业总收入
        Column("ebit_of_gr", Float),  # 息税前利润/营业总收入
        Column("roe", Float),  # 净资产收益率
        Column("roe_waa", Float),  # 加权平均净资产收益率
        Column("roe_dt", Float),  # 净资产收益率(扣除非经常损益)
        Column("roa", Float),  # 总资产报酬率
        Column("npta", Float),  # 总资产净利润
        Column("roic", Float),  # 投入资本回报率
        Column("roe_yearly", Float),  # 年化净资产收益率
        Column("roa2_yearly", Float),  # 年化总资产报酬率
        Column("roe_avg", Float),  # 平均净资产收益率(增发条件)
        Column("opincome_of_ebt", Float),  # 经营活动净收益/利润总额
        Column("investincome_of_ebt", Float),  # 价值变动净收益/利润总额
        Column("n_op_profit_of_ebt", Float),  # 营业外收支净额/利润总额
        Column("tax_to_ebt", Float),  # 所得税/利润总额
        Column("dtprofit_to_profit", Float),  # 扣除非经常损益后的净利润/净利润
        Column("salescash_to_or", Float),  # 销售商品提供劳务收到的现金/营业收入
        Column("ocf_to_or", Float),  # 经营活动产生的现金流量净额/营业收入
        Column("ocf_to_opincome", Float),  # 经营活动产生的现金流量净额/经营活动净收益
        Column("capitalized_to_da", Float),  # 资本支出/折旧和摊销
        Column("debt_to_assets", Float),  # 资产负债率
        Column("assets_to_eqt", Float),  # 权益乘数
        Column("dp_assets_to_eqt", Float),  # 权益乘数(杜邦分析)
        Column("ca_to_assets", Float),  # 流动资产/总资产
        Column("nca_to_assets", Float),  # 非流动资产/总资产
        Column("tbassets_to_totalassets", Float),  # 有形资产/总资产
        Column("int_to_talcap", Float),  # 带息债务/全部投入资本
        Column("eqt_to_talcapital", Float),  # 归属于母公司的股东权益/全部投入资本
        Column("currentdebt_to_debt", Float),  # 流动负债/负债合计
        Column("longdeb_to_debt", Float),  # 非流动负债/负债合计
        Column("ocf_to_shortdebt", Float),  # 经营活动产生的现金流量净额/流动负债
        Column("debt_to_eqt", Float),  # 产权比率
        Column("eqt_to_debt", Float),  # 归属于母公司的股东权益/负债合计
        Column("eqt_to_interestdebt", Float),  # 归属于母公司的股东权益/带息债务
        Column("tangibleasset_to_debt", Float),  # 有形资产/负债合计
        Column("tangasset_to_intdebt", Float),  # 有形资产/带息债务
        Column("tangibleasset_to_netdebt", Float),  # 有形资产/净债务
        Column("ocf_to_debt", Float),  # 经营活动产生的现金流量净额/负债合计
        Column("ocf_to_interestdebt", Float),  # 经营活动产生的现金流量净额/带息债务
        Column("ocf_to_netdebt", Float),  # 经营活动产生的现金流量净额/净债务
        Column("ebit_to_interest", Float),  # 已获利息倍数(EBIT/利息费用)
        Column("longdebt_to_workingcapital", Float),  # 长期债务与营运资金比率
        Column("ebitda_to_debt", Float),  # 息税折旧摊销前利润/负债合计
        Column("turn_days", Float),  # 营业周期
        Column("roa_yearly", Float),  # 年化总资产净利率
        Column("roa_dp", Float),  # 总资产净利率(杜邦分析)
        Column("fixed_assets", Float),  # 固定资产合计
        Column("profit_prefin_exp", Float),  # 扣除财务费用前营业利润
        Column("non_op_profit", Float),  # 非营业利润
        Column("op_to_ebt", Float),  # 营业利润／利润总额
        Column("nop_to_ebt", Float),  # 非营业利润／利润总额
        Column("ocf_to_profit", Float),  # 经营活动产生的现金流量净额／营业利润
        Column("cash_to_liqdebt", Float),  # 货币资金／流动负债
        Column("cash_to_liqdebt_withinterest", Float),  # 货币资金／带息流动负债
        Column("op_to_liqdebt", Float),  # 营业利润／流动负债
        Column("op_to_debt", Float),  # 营业利润／负债合计
        Column("roic_yearly", Float),  # 年化投入资本回报率
        Column("total_fa_trun", Float),  # 固定资产合计周转率
        Column("profit_to_op", Float),  # 利润总额／营业收入
        Column("q_opincome", Float),  # 经营活动单季度净收益
        Column("q_investincome", Float),  # 价值变动单季度净收益
        Column("q_dtprofit", Float),  # 扣除非经常损益后的单季度净利润
        Column("q_eps", Float),  # 每股收益(单季度)
        Column("q_netprofit_margin", Float),  # 销售净利率(单季度)
        Column("q_gsprofit_margin", Float),  # 销售毛利率(单季度)
        Column("q_exp_to_sales", Float),  # 销售期间费用率(单季度)
        Column("q_profit_to_gr", Float),  # 净利润／营业总收入(单季度)
        Column("q_saleexp_to_gr", Float),  # 销售费用／营业总收入 (单季度)
        Column("q_adminexp_to_gr", Float),  # 管理费用／营业总收入 (单季度)
        Column("q_finaexp_to_gr", Float),  # 财务费用／营业总收入 (单季度)
        Column("q_impair_to_gr_ttm", Float),  # 资产减值损失／营业总收入(单季度)
        Column("q_gc_to_gr", Float),  # 营业总成本／营业总收入 (单季度)
        Column("q_op_to_gr", Float),  # 营业利润／营业总收入(单季度)
        Column("q_roe", Float),  # 净资产收益率(单季度)
        Column("q_dt_roe", Float),  # 净资产单季度收益率(扣除非经常损益)
        Column("q_npta", Float),  # 总资产净利润(单季度)
        Column("q_opincome_to_ebt", Float),  # 经营活动净收益／利润总额(单季度)
        Column("q_investincome_to_ebt", Float),  # 价值变动净收益／利润总额(单季度)
        Column("q_dtprofit_to_profit", Float),  # 扣除非经常损益后的净利润／净利润(单季度)
        Column("q_salescash_to_or", Float),  # 销售商品提供劳务收到的现金／营业收入(单季度)
        Column("q_ocf_to_sales", Float),  # 经营活动产生的现金流量净额／营业收入(单季度)
        Column("q_ocf_to_or", Float),  # 经营活动产生的现金流量净额／经营活动净收益(单季度)
        Column("basic_eps_yoy", Float),  # 基本每股收益同比增长率(%)
        Column("dt_eps_yoy", Float),  # 稀释每股收益同比增长率(%)
        Column("cfps_yoy", Float),  # 每股经营活动产生的现金流量净额同比增长率(%)
        Column("op_yoy", Float),  # 营业利润同比增长率(%)
        Column("ebt_yoy", Float),  # 利润总额同比增长率(%)
        Column("netprofit_yoy", Float),  # 归属母公司股东的净利润同比增长率(%)
        Column("dt_netprofit_yoy", Float),  # 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
        Column("ocf_yoy", Float),  # 经营活动产生的现金流量净额同比增长率(%)
        Column("roe_yoy", Float),  # 净资产收益率(摊薄)同比增长率(%)
        Column("bps_yoy", Float),  # 每股净资产相对年初增长率(%)
        Column("assets_yoy", Float),  # 资产总计相对年初增长率(%)
        Column("eqt_yoy", Float),  # 归属母公司的股东权益相对年初增长率(%)
        Column("tr_yoy", Float),  # 营业总收入同比增长率(%)
        Column("or_yoy", Float),  # 营业收入同比增长率(%)
        Column("q_gr_yoy", Float),  # 营业总收入同比增长率(%)(单季度)
        Column("q_gr_qoq", Float),  # 营业总收入环比增长率(%)(单季度)
        Column("q_sales_yoy", Float),  # 营业收入同比增长率(%)(单季度)
        Column("q_sales_qoq", Float),  # 营业收入环比增长率(%)(单季度)
        Column("q_op_yoy", Float),  # 营业利润同比增长率(%)(单季度)
        Column("q_op_qoq", Float),  # 营业利润环比增长率(%)(单季度)
        Column("q_profit_yoy", Float),  # 净利润同比增长率(%)(单季度)
        Column("q_profit_qoq", Float),  # 净利润环比增长率(%)(单季度)
        Column("q_netprofit_yoy", Float),  # 归属母公司股东的净利润同比增长率(%)(单季度)
        Column("q_netprofit_qoq", Float),  # 归属母公司股东的净利润环比增长率(%)(单季度)
        Column("equity_yoy", Float),  # 净资产同比增长率
        Column("rd_exp", Float)  # 研发费用

    )

def get_ts_code(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code from stock_basic', engine)

def get_ts_code_and_list_date(engine):
    """查询ts_code"""
    return pd.read_sql('select ts_code,list_date from stock_basic', engine)


def update_bulk_fina_indicator_by_period_and_ts_code(base_name, engine, pro, codes, start_date, end_date, retry_count=3,
                                             pause=2):
    coverage = dbUtil.getTableRange(base_name="", start_date=start_date, end_date=end_date)
    for i in coverage:
        for rownum in range(0, len(codes)):
            logger.debug("started processing data for " + codes.iloc[rownum]['ts_code'] + " for period " + i)
            if (int(codes.iloc[rownum]['list_date'][0:4]) <= int(i[1:5]) or int(
                    codes.iloc[rownum]['list_date'][0:4]) <= int(i[6:10])):
                try:
                    to_insert = pro.fina_indicator_vip(ts_code=codes.iloc[rownum]['ts_code'], start_date=i[1:5] + '0101',
                                               end_date=i[6:10] + '1231', fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp')
                    logger.debug("start inserting data into DB")
                    to_insert.to_sql(base_name + i, engine, if_exists='append', index=False)
                    logger.debug("end inserting data into DB")
                except Exception as e:
                    logger.error(e)
                    logger.error(
                        "error processing data for range " + str(i) + " for code " + codes.iloc[rownum]['ts_code'])


def update_bulk_fina_indicator_by_ts_code_and_insert_by_year(base_name, engine, pro, codes, sharding_column, failed_count=0,
                                                     failed_tolerent=3):
    failed = []
    for code in codes['ts_code']:
        logger.debug("started processing data for " + code)
        try:
            to_insert = pro.fina_indicator_vip(ts_code=code,  fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp')
            logger.debug("start inserting data into DB")
            distinct_years = set(to_insert[sharding_column].str[0:4])
            for year in distinct_years:
                year_section = to_insert[to_insert[sharding_column].str[0:4] == year]
                if (year == None):
                    year = 9999
                    year_section = to_insert[pd.isna(to_insert[sharding_column]) == True]
                year_section.to_sql(dbUtil.getTableName(int(year), base_name=base_name), engine, if_exists='append',
                                    index=False)
            logger.debug("end inserting data into DB")
        except Exception as e:
            failed.append(code)
            logger.error(e)
            logger.error("error processing data for code " + code)
    if (failed_count < failed_tolerent):
        logger.warning("retrying now.")
        failed_count = failed_count + 1
        update_bulk_fina_indicator_by_ts_code_and_insert_by_year(base_name=base_name, engine=engine, pro=pro,
                                                         codes=pd.DataFrame(failed, columns=['ts_code']),
                                                         sharding_column=sharding_column,
                                                         failed_count=failed_count)
    else:
        logger.error("the below code has failed after maximum attempts. " + ','.join(failed))

logger = logging.getLogger('fina_indicator_sharded')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setStream(sys.stdout)
fi = logging.FileHandler(filename="../engine.log")

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fi.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
logger.addHandler(fi)

engine = create_engine(config_service.getDefaultDB())
conn = engine.connect()

metadata = MetaData()

years = dbUtil.getYears()
for i in years.keys():
    getTableMeta(i, metadata)

metadata.create_all(engine)

# df = get_ts_code_and_list_date(engine)
df = get_ts_code(engine)
# update_bulk_fina_indicator_by_period_and_ts_code(base_name='fina_indicator', engine=engine, pro=pro, codes=df, start_date='19950101',
# end_date=datetime.date.today().strftime("%Y%m%d"))
update_bulk_fina_indicator_by_ts_code_and_insert_by_year(base_name='fina_indicator', engine=engine, pro=pro, codes=df,
                                                 sharding_column='ann_date')
