import pandas as pd
from loguru import logger
from rich.progress import track
from husfort.qutility import check_and_makedirs, SFG, qtimer
from husfort.qcalendar import CCalendar
from husfort.qsqlite import CDbStruct, CMgrSqlDb


class CPosInstru:
    def __init__(self, instrument: str, src_db_struct: CDbStruct, dst_db_struct: CDbStruct):
        self.instrument = instrument
        self.src_db_struct = src_db_struct
        self.dst_db_struct = dst_db_struct

    def load(self, bgn_date: str, stp_date: str) -> pd.DataFrame:
        sqldb = CMgrSqlDb(
            db_save_dir=self.src_db_struct.db_save_dir,
            db_name=self.src_db_struct.db_name,
            table=self.src_db_struct.table,
            mode="r"
        )
        data = sqldb.read_by_instrument_range(bgn_date=bgn_date, stp_date=stp_date, instrument=self.instrument)
        return data

    def align_dates(self, new_data: pd.DataFrame, bgn_date: str, stp_date: str, calendar: CCalendar) -> pd.DataFrame:
        if new_data.empty:
            logger.info(f"There is no data for {SFG(self.instrument)} from {SFG(bgn_date)} to {SFG(stp_date)}")
        dates_header = calendar.get_dates_header(bgn_date, stp_date)
        aligned_data = pd.merge(
            left=dates_header,
            right=new_data,
            on="trade_date",
            how="left",
        )
        return aligned_data

    def save(self, aligned_data: pd.DataFrame, calendar: CCalendar):
        sqldb = CMgrSqlDb(
            db_save_dir=self.dst_db_struct.db_save_dir,
            db_name=self.dst_db_struct.db_name,
            table=self.dst_db_struct.table,
            mode="a"
        )
        if sqldb.check_continuity(incoming_date=aligned_data["trade_date"].iloc[0], calendar=calendar) == 0:
            sqldb.update(update_data=aligned_data)
        return 0

    def main_position(self, bgn_date: str, stp_date: str, calendar: CCalendar):
        new_data = self.load(bgn_date, stp_date)
        aligned_data = self.align_dates(new_data, bgn_date, stp_date, calendar)
        self.save(aligned_data, calendar)
        return 0


@qtimer
def main_position_by_instru(
        universe: list[str],
        bgn_date: str, stp_date: str, calendar: CCalendar,
        pos_db_struct: CDbStruct, pos_by_instru_save_dir: str
):
    check_and_makedirs(pos_by_instru_save_dir)
    for instru in track(universe, description=f"Splitting {SFG('positions')} to instruments"):
        instru_pos_db_struct = pos_db_struct.copy_to_another(pos_by_instru_save_dir, another_db_name=f"{instru}.db")
        instru_pos = CPosInstru(instru, src_db_struct=pos_db_struct, dst_db_struct=instru_pos_db_struct)
        instru_pos.main_position(bgn_date, stp_date, calendar)
    return 0
