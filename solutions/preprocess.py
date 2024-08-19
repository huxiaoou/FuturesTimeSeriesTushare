import multiprocessing as mp
import pandas as pd
from loguru import logger
from rich.progress import track, Progress
from husfort.qutility import qtimer, SFG, SFY, error_handler, check_and_makedirs
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CMgrSqlDb, CDbStruct
from solutions.shared import load_fmd

"""
Part I: pre_price
"""


def get_pre_price(instru_md_data: pd.DataFrame, price: str = "open") -> pd.DataFrame:
    """
    params: instru_md_data: a pd.DataFrame with columns = ["trade_date", "ticker", price] at least
    params: price: must be in  basic_inputs, default = "open"

    return : a pd.DataFrame with columns = ["trade_date", "ticker", f"pre_{price}"]

    """
    pivot_data = pd.pivot_table(
        data=instru_md_data,
        index="trade_date",
        columns="ticker",
        values=price,
        aggfunc=pd.Series.mean,
    )
    pivot_pre_data = pivot_data.sort_index(ascending=True).shift(1)
    pre_price_data = pivot_pre_data.stack().reset_index().rename(mapper={0: f"pre_{price}"}, axis=1)
    return pre_price_data


"""
Part II: load fundamental data
"""


def load_basis(db_struct: CDbStruct, instrument: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct.db_save_dir,
        db_name=db_struct.db_name,
        table=db_struct.table,
        mode="r",
    )
    raw_data = sqldb.read_by_conditions(conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
        ("ts_code", "=", instrument),
    ])
    return raw_data[["trade_date", "basis", "basis_rate", "basis_annual"]]


def load_stock(db_struct: CDbStruct, instrument: str, bgn_date: str, stp_date: str,
               dates_header: pd.DataFrame) -> pd.DataFrame:
    # sqldb = CMgrSqlDb(
    #     db_save_dir=db_struct.db_save_dir,
    #     db_name=db_struct.db_name,
    #     table=db_struct.table,
    #     mode="r",
    # )
    # raw_data = sqldb.read_by_conditions(conditions=[
    #     ("trade_date", ">=", bgn_date),
    #     ("trade_date", "<", stp_date),
    #     ("ts_code", "=", instrument),
    # ])
    # return raw_data[["trade_date", "in_stock"]]
    return pd.DataFrame({"trade_date": dates_header["trade_date"], "in_stock": 0})


"""
Part III: found major and minor ticker
"""


def find_major_and_minor_by_instru(
        instru: str, instru_all_data: pd.DataFrame, vol_alpha: float, slc_vars: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    return: 2 pd.DataFrames with cols
            first:  ["trade_date", "ticker"] + basic_inputs + major
            second:  ["trade_date", "ticker"] + basic_inputs + major

    """

    def __reformat(raw_data: pd.DataFrame, reformat_vars: list[str]):
        if raw_data.empty:
            return pd.DataFrame(columns=reformat_vars)
        else:
            raw_data = raw_data.reset_index().rename(mapper={"index": "ticker"}, axis=1)
            return raw_data[reformat_vars]

    major_res, minor_res = [], []
    if not instru_all_data.empty:
        wgt = pd.Series({"oi": 1 - vol_alpha, "vol": vol_alpha})
        instru_all_data["oi_add_vol"] = instru_all_data[["oi", "vol"]].fillna(0) @ wgt
        instru_all_data = instru_all_data.sort_values(
            by=["trade_date", "oi_add_vol", "ticker"],
            ascending=[True, False, True],
        ).set_index("ticker")
        for trade_date, trade_date_instru_data in instru_all_data.groupby(by="trade_date"):
            sv = trade_date_instru_data["oi_add_vol"]  # a pd.Series: sum of oi and vol, with contract_id as index
            major_ticker = sv.idxmax()
            minor_sv = sv[sv.index > major_ticker]
            if not minor_sv.empty:
                minor_ticker = minor_sv.idxmax()
            else:
                minor_sv = sv[sv.index < major_ticker]
                if not minor_sv.empty:
                    minor_ticker = minor_sv.idxmax()
                    # always keep major_ticker is ahead of minor_ticker
                    major_ticker, minor_ticker = minor_ticker, major_ticker
                else:
                    minor_ticker = major_ticker
                    logger.warning(f"There is only one ticker for {SFY(instru)} at {SFG(trade_date)}")
            s_major = trade_date_instru_data.loc[major_ticker]
            s_minor = trade_date_instru_data.loc[minor_ticker]
            major_res.append(s_major)
            minor_res.append(s_minor)
    major_data, minor_data = pd.DataFrame(major_res), pd.DataFrame(minor_res)
    rft_vars = ["trade_date", "ticker"] + slc_vars
    major_data, minor_data = __reformat(major_data, rft_vars), __reformat(minor_data, rft_vars)
    return major_data, minor_data


"""
Part IV: found vol, amount, oi by instrument
"""


def sum_vol_amount_oi_by_instru(instru_all_data: pd.DataFrame) -> pd.DataFrame:
    vol_amount_oi_cols = ["vol", "amount", "oi"]
    save_cols = ["trade_date"] + vol_amount_oi_cols
    if not instru_all_data.empty:
        sum_df = pd.pivot_table(
            data=instru_all_data,
            index=["trade_date"],
            values=vol_amount_oi_cols,
            aggfunc="sum",
        )
        sum_df = sum_df.reset_index()
        return sum_df[save_cols]
    else:
        return pd.DataFrame(columns=save_cols)


"""
Part V: Merge all
"""


def merge_all(
        dates_header: pd.DataFrame,
        instru_pre_open_data: pd.DataFrame,
        instru_maj_data: pd.DataFrame,
        instru_min_data: pd.DataFrame,
        instru_vol_data: pd.DataFrame,
        instru_basis_data: pd.DataFrame,
        instru_stock_data: pd.DataFrame,
) -> pd.DataFrame:
    keys = "trade_date"
    instru_maj_data_plus_open = pd.merge(
        left=instru_maj_data, right=instru_pre_open_data, on=["trade_date", "ticker"], how="left",
    )
    instru_min_data_plus_open = pd.merge(
        left=instru_min_data, right=instru_pre_open_data, on=["trade_date", "ticker"], how="left",
    )
    merged_data = pd.merge(left=dates_header, right=instru_maj_data_plus_open, on=keys, how="left")
    merged_data = merged_data.merge(right=instru_min_data_plus_open, on=keys, how="left", suffixes=("_major", "_minor"))
    merged_data = merged_data.merge(right=instru_vol_data, on=keys, how="left")
    merged_data = merged_data.merge(right=instru_basis_data, on=keys, how="left")
    merged_data = merged_data.merge(right=instru_stock_data, on=keys, how="left")
    return merged_data


"""
Part VI: Final adjustment
"""


def make_double_to_single(
        instru_data: pd.DataFrame,
        adj_cols: list[str],
        adj_date: str,
        instru: str,
        exempt_instruments: list[str],
):
    if instru in exempt_instruments:
        adj_ratio = 1
    else:
        adj_ratio = [2 if t < adj_date else 1 for t in instru_data["trade_date"]]
    instru_data[adj_cols] = instru_data[adj_cols].div(adj_ratio, axis="index").fillna(0)
    return 0


def adjust_and_select(instru: str, merged_data: pd.DataFrame, output_vars: list[str]) -> pd.DataFrame:
    _vol_adj_date: str = "20200101"
    _exempt_instruments = ["IH", "IF", "IC", "IM", "TF", "TS", "T", "TL"]

    # adjust volume, amount and oi cols
    _vol_cols = ["vol", "amount", "oi"]
    maj_cols = [f"{z}_major" for z in _vol_cols]
    min_cols = [f"{z}_minor" for z in _vol_cols]
    vol_cols = [f"{z}_instru" for z in _vol_cols]
    adj_cols = maj_cols + min_cols + vol_cols
    merged_data.rename(mapper={_z: z for _z, z in zip(_vol_cols, vol_cols)}, axis=1, inplace=True)
    make_double_to_single(
        instru_data=merged_data,
        adj_cols=adj_cols,
        adj_date=_vol_adj_date,
        instru=instru,
        exempt_instruments=_exempt_instruments,
    )

    # selected data
    selected_data = merged_data[output_vars]
    return selected_data


def process_for_instru(
        instru: str,
        bgn_date: str,
        stp_date: str,
        vol_alpha: float,
        slc_vars: list[str],
        db_struct_fmd: CDbStruct,
        db_struct_basis: CDbStruct,
        db_struct_stock: CDbStruct,
        db_struct_preprocess: CDbStruct,
        calendar: CCalendar,
):
    dates_header = calendar.get_dates_header(bgn_date, stp_date)

    # load
    base_bgn_date = calendar.get_next_date(bgn_date, -1)
    instru_all_data = load_fmd(db_struct_fmd, instru, base_bgn_date, stp_date)
    instru_basis_data = load_basis(db_struct_basis, instru, bgn_date, stp_date)
    instru_stock_data = load_stock(db_struct_stock, instru, bgn_date, stp_date, dates_header)

    # to sql
    check_and_makedirs(db_struct_preprocess.db_save_dir)
    db_struct_instru = db_struct_preprocess.copy_to_another(another_db_name=f"{instru}.db")
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_instru.db_save_dir,
        db_name=db_struct_instru.db_name,
        table=db_struct_instru.table,
        mode="a",
    )
    if sqldb.check_continuity(bgn_date, calendar) == 0:
        instru_pre_open_data = get_pre_price(instru_all_data, price="open")
        instru_maj_data, instru_min_data = find_major_and_minor_by_instru(
            instru=instru,
            instru_all_data=instru_all_data,
            slc_vars=slc_vars,
            vol_alpha=vol_alpha,
        )
        instru_vol_data = sum_vol_amount_oi_by_instru(instru_all_data=instru_all_data)
        merged_data = merge_all(
            dates_header=dates_header,
            instru_pre_open_data=instru_pre_open_data,
            instru_maj_data=instru_maj_data,
            instru_min_data=instru_min_data,
            instru_vol_data=instru_vol_data,
            instru_basis_data=instru_basis_data,
            instru_stock_data=instru_stock_data,
        )
        new_data = adjust_and_select(
            instru=instru,
            merged_data=merged_data,
            output_vars=db_struct_instru.table.vars.names,
        )
        sqldb.update(update_data=new_data)
    return 0


@qtimer
def main_preprocess(
        universe: list[str],
        bgn_date: str,
        stp_date: str,
        vol_alpha: float,
        db_struct_fmd: CDbStruct,
        db_struct_basis: CDbStruct,
        db_struct_stock: CDbStruct,
        db_struct_preprocess: CDbStruct,
        slc_vars: list[str],
        calendar: CCalendar,
        call_multiprocess: bool,
):
    # header
    if call_multiprocess:
        with Progress() as pb:
            main_task = pb.add_task(description=f"[INF] Preprocessing {bgn_date}->{stp_date}", total=len(universe))
            with mp.get_context("spawn").Pool() as pool:
                for instru in universe:
                    pool.apply_async(
                        process_for_instru,
                        kwds={
                            "instru": instru,
                            "bgn_date": bgn_date,
                            "stp_date": stp_date,
                            "vol_alpha": vol_alpha,
                            "slc_vars": slc_vars,
                            "db_struct_fmd": db_struct_fmd,
                            "db_struct_basis": db_struct_basis,
                            "db_struct_stock": db_struct_stock,
                            "db_struct_preprocess": db_struct_preprocess,
                            "calendar": calendar,
                        },
                        callback=lambda _: pb.update(main_task, advance=1),
                        error_callback=error_handler,
                    )
                pool.close()
                pool.join()
    else:
        for instru in track(universe, description=f"Preprocessing {bgn_date}->{stp_date}"):
            # for instru in universe:
            process_for_instru(
                instru=instru,
                bgn_date=bgn_date,
                stp_date=stp_date,
                vol_alpha=vol_alpha,
                slc_vars=slc_vars,
                db_struct_fmd=db_struct_fmd,
                db_struct_basis=db_struct_basis,
                db_struct_stock=db_struct_stock,
                db_struct_preprocess=db_struct_preprocess,
                calendar=calendar,
            )
    return 0
