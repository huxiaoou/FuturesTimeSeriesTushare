"""
0. 另类数据: 汇率, CPI, M2, PPI
1. 保存路径为 E:\\Onedrive\\Data\\Alternative

alternative.db/forex:
| trade_date  | preclose |  open  |  high  |  low   | close  | pct_chg |
|-------------|----------|--------|--------|--------|--------|---------|
|    20240318 |   7.1965 | 7.1940 | 7.1986 | 7.1940 | 7.1982 |  0.0236 |
|    20240319 |   7.1982 | 7.1950 | 7.1997 | 7.1950 | 7.1993 |  0.0153 |
|    20240320 |   7.1993 | 7.1962 | 7.1998 | 7.1962 | 7.1993 |  0.0000 |
|    20240321 |   7.1993 | 7.1920 | 7.2002 | 7.1920 | 7.1994 |  0.0014 |
|    20240322 |   7.1994 | 7.1950 | 7.2304 | 7.1950 | 7.2283 |  0.4014 |

alternative.db/macro:
| trade_date  | cpi_rate | m2_rate | ppi_rate |
|-------------|----------|---------|----------|
|    20240318 |   -0.800 |   8.700 |   -2.500 |
|    20240319 |   -0.800 |   8.700 |   -2.500 |
|    20240320 |   -0.800 |   8.700 |   -2.500 |
|    20240321 |   -0.800 |   8.700 |   -2.500 |
|    20240322 |   -0.800 |   8.700 |   -2.500 |

"""

import argparse


def parse_args():
    arg_parser = argparse.ArgumentParser(description="To calculate data, such as macro and forex")
    arg_parser.add_argument(
        "--switch", type=str,
        choices=("macro", "forex", "position", "preprocess"),
        required=True
    )
    arg_parser.add_argument("--bgn", type=str, help="begin date, format = [YYYYMMDD]", required=True)
    arg_parser.add_argument("--stp", type=str, help="stop  date, format = [YYYYMMDD]")
    arg_parser.add_argument("--nomp", default=False, action="store_true",
                            help="not using multiprocess, for debug. Works only when switch in ('preprocess',)")
    return arg_parser.parse_args()


if __name__ == "__main__":
    from project_cfg import pro_cfg, db_struct_cfg
    from husfort.qlog import define_logger
    from husfort.qcalendar import CCalendar

    define_logger()

    calendar = CCalendar(pro_cfg.calendar_path)
    args = parse_args()
    bgn_date, stp_date = args.bgn, args.stp or calendar.get_next_date(args.bgn, shift=1)

    if args.switch == "macro":
        from solutions.alternative import main_macro

        main_macro(
            bgn_date=bgn_date,
            stp_date=stp_date,
            path_macro_data=pro_cfg.path_macro_data,
            alternative_dir=pro_cfg.alternative_dir,
            db_struct_macro=db_struct_cfg.macro,
            calendar=calendar,
        )
    elif args.switch == "forex":
        from solutions.alternative import main_forex

        main_forex(
            bgn_date=bgn_date,
            stp_date=stp_date,
            path_forex_data=pro_cfg.path_forex_data,
            alternative_dir=pro_cfg.alternative_dir,
            db_struct_forex=db_struct_cfg.forex,
            calendar=calendar,
        )
    elif args.switch == "position":
        from solutions.position import main_position_by_instru

        main_position_by_instru(
            universe=pro_cfg.universe,
            bgn_date=bgn_date,
            stp_date=stp_date,
            calendar=calendar,
            pos_db_struct=db_struct_cfg.position,
            pos_by_instru_save_dir=pro_cfg.by_instru_pos_dir,
        )
    elif args.switch == "preprocess":
        from solutions.preprocess import main_preprocess

        slc_vars = [
            "pre_close", "pre_settle",
            "open", "high", "low", "close",
            "vol", "amount", "oi",
        ]
        main_preprocess(
            universe=pro_cfg.universe,
            bgn_date=bgn_date,
            stp_date=stp_date,
            vol_alpha=pro_cfg.vol_alpha,
            db_struct_fmd=db_struct_cfg.fmd,
            db_struct_basis=db_struct_cfg.basis,
            db_struct_stock=db_struct_cfg.stock,
            db_struct_preprocess=db_struct_cfg.preprocess,
            slc_vars=slc_vars,
            calendar=calendar,
            call_multiprocess=not args.nomp,
        )
    else:
        raise ValueError(f"args.switch = {args.switch} is illegal")
