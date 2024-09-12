import os
import datetime as dt
import multiprocessing as mp
import numpy as np
import pandas as pd
from loguru import logger
from rich.progress import track, Progress
from husfort.qutility import SFG, SFR, check_and_makedirs, error_handler
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb

logger.add(f"logs/minute_bar_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


class CMinuteBarInstru:
    def __init__(
            self, instrument: str, src_data_root_dir: str, src_data_file_name_tmpl: str,
            preprocess_db_struct: CDbStruct, dst_db_struct: CDbStruct
    ):
        self.instrument = instrument
        self.src_data_file_name_tmpl = src_data_file_name_tmpl
        self.src_data_root_dir = src_data_root_dir
        self.preprocess_db_struct = preprocess_db_struct
        self.dst_db_struct = dst_db_struct

        self.major_ticker_data: pd.DataFrame = pd.DataFrame()

    def init_major_ticker(self, bgn_date: str, stp_date: str) -> None:
        sqldb = CMgrSqlDb(
            db_save_dir=self.preprocess_db_struct.db_save_dir,
            db_name=self.preprocess_db_struct.db_name,
            table=self.preprocess_db_struct.table,
            mode="r",
        )
        data = sqldb.read_by_range(bgn_date, stp_date, value_columns=["trade_date", "ticker_major"])
        self.major_ticker_data = data.set_index("trade_date")

    def load_minute_data(self, trade_date: str, contract: str) -> pd.DataFrame:
        src_file = self.src_data_file_name_tmpl.format(trade_date)
        src_path = os.path.join(self.src_data_root_dir, trade_date[0:4], trade_date, src_file)
        if os.path.exists(src_path):
            minute_data = pd.read_csv(src_path, dtype={"trade_date": str, "timestamp": str})
            contract_minute_data = minute_data.query(f"ts_code == '{contract}'")
        else:
            contract_minute_data = pd.DataFrame()
        if contract_minute_data.empty:
            logger.info(f"There is no minute data for {SFR(trade_date)}/{SFR(contract)}")
        return contract_minute_data

    def get_ticker_major(self, trade_date: str) -> str:
        return self.major_ticker_data.at[trade_date, "ticker_major"]

    @staticmethod
    def add_prev_price(prev_minute_data: pd.DataFrame, this_minute_data: pd.DataFrame) -> pd.DataFrame:
        if this_minute_data.empty:
            return pd.DataFrame()
        if prev_minute_data.empty:
            pre_open, pre_close = np.nan, np.nan
        else:
            pre_open, pre_close = prev_minute_data["open"].iloc[-1], prev_minute_data["close"].iloc[-1]
        raw_data = this_minute_data.reset_index(drop=True)
        raw_data["pre_open"] = raw_data["open"].shift(1)
        raw_data["pre_close"] = raw_data["close"].shift(1)
        raw_data.at[0, "pre_open"] = pre_open
        raw_data.at[0, "pre_close"] = pre_close
        return raw_data

    def reformat(self, raw_data: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        if raw_data.empty:
            return pd.DataFrame()

        # --- timestamp
        raw_data["timestamp"] = raw_data["timestamp"].map(
            lambda z: dt.datetime.strptime(z, "%Y-%m-%d %H:%M:%S").timestamp()).astype("int64")

        # --- vol adjustment
        _vol_adj_date: str = "20200101"
        _exempt_instruments = ["IH", "IF", "IC", "IM", "TF", "TS", "T", "TL"]
        _vol_cols = ["vol", "amount", "oi"]
        instru, exchange = self.instrument.split(".")
        if (instru not in _exempt_instruments) and (trade_date < _vol_adj_date):
            raw_data[_vol_cols] = raw_data[_vol_cols] / 2
        return raw_data[self.dst_db_struct.table.vars.names]

    def save(self, instru_minute_data: pd.DataFrame, calendar: CCalendar) -> None:
        sqldb = CMgrSqlDb(
            db_save_dir=self.dst_db_struct.db_save_dir,
            db_name=self.dst_db_struct.db_name,
            table=self.dst_db_struct.table,
            mode="a",
        )
        if sqldb.check_continuity(incoming_date=instru_minute_data["trade_date"].iloc[0], calendar=calendar) <= 1:
            sqldb.update(update_data=instru_minute_data)

    def main(self, bgn_date: str, stp_date: str, calendar: CCalendar) -> None:
        iter_dates = calendar.get_iter_list(bgn_date, stp_date)
        prev_dates = [calendar.get_next_date(iter_dates[0], -1)] + iter_dates[:-1]
        self.init_major_ticker(bgn_date=bgn_date, stp_date=stp_date)
        dfs: list[pd.DataFrame] = []
        for this_date, prev_date in zip(iter_dates, prev_dates):
            major_ticker = self.get_ticker_major(trade_date=this_date)
            if major_ticker is None:
                logger.info(f"There is no ticker for {SFR(this_date)}/{SFR(self.instrument)}")
                continue
            prev_minute_data = self.load_minute_data(trade_date=prev_date, contract=major_ticker)
            this_minute_data = self.load_minute_data(trade_date=this_date, contract=major_ticker)
            raw_data = self.add_prev_price(prev_minute_data, this_minute_data)
            new_data = self.reformat(raw_data, trade_date=this_date)
            if not new_data.empty:
                dfs.append(new_data)
        if dfs:
            instru_minute_data = pd.concat(dfs, axis=0, ignore_index=True)
            self.save(instru_minute_data, calendar)


def main_minute_bar(
        universe: list[str],
        src_data_root_dir: str,
        src_data_file_name_tmpl: str,
        db_struct_preprocess: CDbStruct,
        db_struct_minute_bar: CDbStruct,
        bgn_date: str, stp_date: str, calendar: CCalendar,
        call_multiprocess: bool,
        processes: int,
) -> None:
    check_and_makedirs(db_struct_minute_bar.db_save_dir)
    minute_bar_instruments = [
        CMinuteBarInstru(
            instrument=instru,
            src_data_root_dir=src_data_root_dir,
            src_data_file_name_tmpl=src_data_file_name_tmpl,
            preprocess_db_struct=db_struct_preprocess.copy_to_another(another_db_name=f"{instru}.db"),
            dst_db_struct=db_struct_minute_bar.copy_to_another(another_db_name=f"{instru}.db")
        ) for instru in universe
    ]

    desc = f"Creating major {SFG('minute bar')} by instruments"
    if call_multiprocess:
        with Progress() as pb:
            task_id = pb.add_task(description=desc, total=len(universe))
            with mp.get_context("spawn").Pool(processes) as pool:
                for minute_bar_instru in minute_bar_instruments:
                    pool.apply_async(
                        minute_bar_instru.main,
                        args=(bgn_date, stp_date, calendar),
                        callback=lambda _: pb.update(task_id, advance=1),
                        error_callback=error_handler,
                    )
                pool.close()
                pool.join()
    else:
        for minute_bar_instru in track(minute_bar_instruments, description=desc):
            minute_bar_instru.main(bgn_date, stp_date, calendar)
