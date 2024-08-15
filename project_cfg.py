import yaml
from dataclasses import dataclass
from husfort.qsqlite import CDbStruct, CSqlTable


# ---------- project configuration ----------

@dataclass(frozen=True)
class CProCfg:
    calendar_path: str
    path_macro_data: str
    path_forex_data: str
    root_dir: str
    daily_data_root_dir: str
    db_struct_path: str
    alternative_dir: str


pro_cfg = CProCfg(
    calendar_path=r"D:\OneDrive\Data\Calendar\cne_calendar.csv",
    path_macro_data=r"D:\OneDrive\Data\Alternative\china_cpi_m2.xlsx",
    path_forex_data=r"D:\OneDrive\Data\Alternative\exchange_rate.xlsx",
    root_dir=r"D:\OneDrive\Data\tushare",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare\by_date",
    db_struct_path=r"D:\OneDrive\Data\tushare\db_struct.yaml",
    alternative_dir=r"D:\OneDrive\Data\Alternative",
)

# ---------- databases structure ----------
with open(pro_cfg.db_struct_path, "r") as f:
    db_struct = yaml.safe_load(f)


@dataclass(frozen=True)
class CDbStructCfg:
    macro: CDbStruct
    forex: CDbStruct


db_struct_cfg = CDbStructCfg(
    macro=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["macro"]["db_name"],
        table=CSqlTable(cfg=db_struct["macro"]["table"]),
    ),
    forex=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["forex"]["db_name"],
        table=CSqlTable(cfg=db_struct["forex"]["table"]),
    )
)
