$bgn_date = Read-Host "Please input the append date, format = [YYYYMMDD]"

python main.py --bgn $bgn_date --switch macro
python main.py --bgn $bgn_date --switch forex
python main.py --bgn $bgn_date --switch position
python main.py --bgn $bgn_date --switch preprocess
