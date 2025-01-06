$bgn_date = "20241202"
$stp_date = "20250101"

python main.py --bgn $bgn_date --stp $stp_date --switch macro
python main.py --bgn $bgn_date --stp $stp_date --switch forex
python main.py --bgn $bgn_date --stp $stp_date --switch position
python main.py --bgn $bgn_date --stp $stp_date --switch preprocess
python main.py --bgn $bgn_date --stp $stp_date --switch minute_bar
