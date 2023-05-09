## Step 1: Create Virtual Environment and Install Dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Note: tested in Python 3.10.6

## Step 2: Create Fake Data
### Create Fake Data

``` bash
python3 gen_fake_data.py
```

## Step 3: Test Speed of Various Operations
### Test Read CSV Speed

``` bash
bash scripts/test_read_csv.sh
```

### Test Join Speed

``` bash
bash scripts/test_join.sh
```

### Test Quintuple Join Speed

``` bash
bash scripts/test_quintuple_join.sh
```

### Test Filter Numerical Column Speed

``` bash
bash scripts/test_filter_by_num.sh
```

### Test Filter Categorical Column Speed

``` bash
bash scripts/test_filter_by_cat.sh
```

### Test Aggregate by Median Speed

``` bash
bash scripts/agg_by_median.sh
```

### Test Aggregate by Mean Speed

``` bash
bash scripts/agg_by_mean.sh
```

### Test Aggregate by Number of Unique Value Speed

``` bash
bash scripts/agg_by_unique.sh
```

### Test GroupBy Speed

``` bash
bash scripts/groupby.sh
```