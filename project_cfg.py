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
    universe: list[str]
    by_instru_pos_dir: str
    by_instru_pre_dir: str
    vol_alpha: float


universe: list[str] = [
    "A.DCE",
    "AG.SHF",
    "AL.SHF",
    "AO.SHF",
    "AP.ZCE",
    "AU.SHF",
    "B.DCE",
    "BB.DCE",
    "BC.INE",
    "BR.SHF",
    "BU.SHF",
    "C.DCE",
    "CF.ZCE",
    "CJ.ZCE",
    "CS.DCE",
    "CU.SHF",
    "CY.ZCE",
    "EB.DCE",
    "EC.INE",
    "EG.DCE",
    "FB.DCE",
    "FG.ZCE",
    "FU.SHF",
    "HC.SHF",
    "I.DCE",
    "IC.CFX",
    "IF.CFX",
    "IH.CFX",
    "IM.CFX",
    "J.DCE",
    "JD.DCE",
    "JM.DCE",
    "JR.ZCE",
    "L.DCE",
    "LC.GFE",
    "LH.DCE",
    "LR.ZCE",
    "LU.INE",
    "M.DCE",
    "MA.ZCE",
    "NI.SHF",
    "NR.INE",
    "OI.ZCE",
    "P.DCE",
    "PB.SHF",
    "PF.ZCE",
    "PG.DCE",
    "PK.ZCE",
    "PM.ZCE",
    "PP.DCE",
    "PX.ZCE",
    "RB.SHF",
    "RI.ZCE",
    "RM.ZCE",
    "RR.DCE",
    "RS.ZCE",
    "RU.SHF",
    "SA.ZCE",
    "SC.INE",
    "SF.ZCE",
    "SH.ZCE",
    "SI.GFE",
    "SM.ZCE",
    "SN.SHF",
    "SP.SHF",
    "SR.ZCE",
    "SS.SHF",
    "T.CFX",
    "TA.ZCE",
    "TF.CFX",
    "TL.CFX",
    "TS.CFX",
    "UR.ZCE",
    "V.DCE",
    "WH.ZCE",
    "WR.SHF",
    "Y.DCE",
    "ZC.ZCE",
    "ZN.SHF",
]  # 79 instruments

pro_cfg = CProCfg(
    calendar_path=r"D:\OneDrive\Data\Calendar\cne_calendar.csv",
    path_macro_data=r"D:\OneDrive\Data\Alternative\china_cpi_m2.xlsx",
    path_forex_data=r"D:\OneDrive\Data\Alternative\exchange_rate.xlsx",
    root_dir=r"D:\OneDrive\Data\tushare",
    daily_data_root_dir=r"D:\OneDrive\Data\tushare\by_date",
    db_struct_path=r"D:\OneDrive\Data\tushare\db_struct.yaml",
    alternative_dir=r"D:\OneDrive\Data\Alternative",
    universe=universe,
    by_instru_pos_dir=r"D:\OneDrive\Data\tushare\by_instrument\position",
    by_instru_pre_dir=r"D:\OneDrive\Data\tushare\by_instrument\preprocess",
    vol_alpha=0.9,
)

# ---------- databases structure ----------
with open(pro_cfg.db_struct_path, "r") as f:
    db_struct = yaml.safe_load(f)


@dataclass(frozen=True)
class CDbStructCfg:
    macro: CDbStruct
    forex: CDbStruct
    fmd: CDbStruct
    position: CDbStruct
    basis: CDbStruct
    stock: CDbStruct
    preprocess: CDbStruct


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
    ),
    fmd=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["fmd"]["db_name"],
        table=CSqlTable(cfg=db_struct["fmd"]["table"]),
    ),
    position=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["position"]["db_name"],
        table=CSqlTable(cfg=db_struct["position"]["table"]),
    ),
    basis=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["basis"]["db_name"],
        table=CSqlTable(cfg=db_struct["basis"]["table"]),
    ),
    stock=CDbStruct(
        db_save_dir=pro_cfg.root_dir,
        db_name=db_struct["stock"]["db_name"],
        table=CSqlTable(cfg=db_struct["stock"]["table"]),
    ),
    preprocess=CDbStruct(
        db_save_dir=pro_cfg.by_instru_pre_dir,
        db_name=db_struct["preprocess"]["db_name"],
        table=CSqlTable(cfg=db_struct["preprocess"]["table"])
    )
)
