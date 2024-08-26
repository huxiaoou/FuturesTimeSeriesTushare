import pandas as pd
from loguru import logger
from husfort.qutility import qtimer, SFG
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CMgrSqlDb, CDbStruct

"""
Part I: Macro data: cpi, m2, ppi
"""


def load_macro_data(path_macro_data: str) -> pd.DataFrame:
    return pd.read_excel(path_macro_data, sheet_name="china_cpi_m2")


def reformat_macro(macro_data: pd.DataFrame, hist_bgn_month: str, calendar: CCalendar) -> pd.DataFrame:
    macro_data["trade_month"] = macro_data["trade_month"].map(lambda z: z.strftime("%Y%m"))
    macro_data["available_month"] = macro_data["trade_month"].map(lambda z: calendar.get_next_month(z, s=2))
    macro_data.set_index(keys="trade_month", inplace=True)
    macro_data = macro_data.truncate(before=hist_bgn_month)
    return macro_data


def merge_macro(reformat_data: pd.DataFrame, dates_header: pd.DataFrame, names: list[str]):
    dates_header["available_month"] = dates_header["trade_date"].map(lambda z: z[0:6])
    res = pd.merge(
        left=dates_header,
        right=reformat_data,
        on="available_month",
        how="left",
    )
    return res[names]


@qtimer
def main_macro(
        bgn_date: str,
        stp_date: str,
        path_macro_data: str,
        db_struct_macro: CDbStruct,
        calendar: CCalendar,
):
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_macro.db_save_dir,
        db_name=db_struct_macro.db_name,
        table=db_struct_macro.table,
        mode="a",
    )
    if sqldb.check_continuity(incoming_date=bgn_date, calendar=calendar) == 0:
        macro_data = load_macro_data(path_macro_data=path_macro_data)
        rft_data = reformat_macro(macro_data=macro_data, hist_bgn_month="201111", calendar=calendar)
        dates_header = calendar.get_dates_header(bgn_date, stp_date)
        new_macro_data = merge_macro(rft_data, dates_header=dates_header, names=db_struct_macro.table.vars.names)
        sqldb.update(update_data=new_macro_data)
        logger.info(f"{SFG('Macro data')} by dates updated")
        print(new_macro_data)
    return 0


"""
Part II: forex: exchange rate
"""


def load_forex_data(path_forex_data: str) -> pd.DataFrame:
    return pd.read_excel(path_forex_data, sheet_name="USDCNY.CFETS")


def reformat_forex(forex_data: pd.DataFrame) -> pd.DataFrame:
    forex_data["trade_date"] = forex_data["Date"].map(lambda z: z.strftime("%Y%m%d"))
    return forex_data


def merge_forex(reformat_data: pd.DataFrame, dates_header: pd.DataFrame, names: list[str]):
    res = pd.merge(
        left=dates_header,
        right=reformat_data,
        on="trade_date",
        how="left",
    )
    res.drop(labels="Date", axis=1, inplace=True)
    return res[names]


@qtimer
def main_forex(
        bgn_date: str,
        stp_date: str,
        path_forex_data: str,
        db_struct_forex: CDbStruct,
        calendar: CCalendar,
):
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_forex.db_save_dir,
        db_name=db_struct_forex.db_name,
        table=db_struct_forex.table,
        mode="a"
    )
    if sqldb.check_continuity(incoming_date=bgn_date, calendar=calendar) == 0:
        forex_data = load_forex_data(path_forex_data=path_forex_data)
        rft_data = reformat_forex(forex_data=forex_data)
        dates_header = calendar.get_dates_header(bgn_date, stp_date)
        new_forex_data = merge_forex(rft_data, dates_header=dates_header, names=db_struct_forex.table.vars.names)
        sqldb.update(update_data=new_forex_data)
        logger.info(f"{SFG('Forex data')} by dates updated")
        print(new_forex_data)
    return 0
