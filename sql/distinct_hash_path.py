import duckdb
import tqdm
import glob
import pathlib
import json
import more_itertools
import sys

input_dir = sys.argv[1]

output = pathlib.Path(sys.argv[2])

cursor = duckdb.connect('').cursor()
x = list(
    more_itertools.batched(
        glob.glob(input_dir),
        2
    )
)
cursor.execute("SET memory_limit='7GB';")
output.mkdir(exist_ok=True, parents=True)

for idx, names in enumerate(tqdm.tqdm(x)):
    fnames = [str(n) for n in names]
    out_name = output / f'{idx}.parquet'
    cursor.execute(f'''
        COPY
        (
        select distinct
        on (hash) path, hash
        from read_parquet({json.dumps(fnames)})
            ) TO '{out_name}' (FORMAT PARQUET, COMPRESSION zstd);
    ''')
