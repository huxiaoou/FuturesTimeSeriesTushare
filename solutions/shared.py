import pandas as pd
from husfort.qsqlite import CMgrSqlDb, CDbStruct


def load_fmd(db_struct_fmd: CDbStruct, instrument: str, bgn_date: str, stp_date: str) -> pd.DataFrame:
    sqldb = CMgrSqlDb(
        db_save_dir=db_struct_fmd.db_save_dir,
        db_name=db_struct_fmd.db_name,
        table=db_struct_fmd.table,
        mode="r",
    )
    raw_data = sqldb.read_by_conditions(conditions=[
        ("trade_date", ">=", bgn_date),
        ("trade_date", "<", stp_date),
        ("instrument", "=", instrument),
    ])
    raw_data.rename(mapper={"ts_code": "ticker"}, axis=1, inplace=True)
    return raw_data
