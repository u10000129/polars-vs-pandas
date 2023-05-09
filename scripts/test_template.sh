FILE=csv/$1.csv
if [ -f $FILE ]; then
    rm $FILE
fi

source venv/bin/activate

for i in {1..20}; do
    python3 test_speed.py -m polars -t $1
    python3 test_speed.py -m pandas2 -t $1
    python3 test_speed.py -m pandas -t $1
done

python3 draw.py -f $1
