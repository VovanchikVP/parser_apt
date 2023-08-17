import os
from pathlib import Path
import pandas as pd


def concat_files_excel(result_file_name: str):
    dir_patch = os.path.dirname(os.path.dirname(__file__)) / Path('result')
    only_files = [os.path.join(dir_patch, f) for f in os.listdir(dir_patch)
                  if os.path.isfile(os.path.join(dir_patch, f))]
    result = {}
    for i in only_files:
        file_name = os.path.basename(i).replace('.xlsx', '')
        xl = pd.ExcelFile(i)
        sheets = xl.sheet_names
        if len(sheets) == 1:
            df = xl.parse(sheets[0], index_col=0)
            result[file_name] = df
            continue

        for sheet in sheets:
            name = f'{file_name}_{sheet}'
            name = name if len(name) < 30 else name[:30]
            df = xl.parse(sheet, index_col=0)
            result[name] = df

    with pd.ExcelWriter(result_file_name) as writer:
        for page_name, value in result.items():
            df = pd.DataFrame(value)
            df.to_excel(writer, sheet_name=page_name)
