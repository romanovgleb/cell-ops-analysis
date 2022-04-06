'''
Script takes in cell operators travel matrix, roughly estimates amount of rows,
reads file in chunks and writes to the new file only the rows, which
timestamps satisfy pre-defined time constraints (specific day/hour selection)
'''

import pandas as pd
import sys # clearing print statements
import time
import os  # approximate rowsize count based on file size

start_time = time.time() # measuring elapsed time

INP_FILENAME = 'CMatrix.csv'
CHUNK = 10 ** 6                                # amount of rows in read chunk
DT_BEG = pd.to_datetime('2021-10-10 00:00:00') # date & time we want to include
DT_END = pd.to_datetime('2021-10-11 00:00:00')
OUT_FILENAME = 'test'
INCLUDE_TIMESTAMPS = True

print('\nEstimated row count = '
      + f'{round(int((os.path.getsize(INP_FILENAME)) / 49.759237), -6):,} rows\n')

# defining output filename
if INCLUDE_TIMESTAMPS:
    out_fname = (f'{OUT_FILENAME} {DT_BEG.strftime("%Y_%m_%d %H_%M")} - '
                 + f'{DT_END.strftime("%Y_%m_%d %H_%M")}.csv')
else:
    out_fname = OUT_FILENAME + '.csv'

# Warn user if output file already exists
if os.path.isfile(out_fname):
    print('Warning! Output file already exists! Make sure it is intentional.'
          + '\nSleeping for 5 seconds\n')
    time.sleep(5)

# Printing out what the program is going to do
print(f'Will be reading "{INP_FILENAME}", checking times between\n'
      + f'{DT_BEG} and {DT_END}\nand appending to "{out_fname}"\n')

# reading file by chunks, filtering dates, appending to csv
header_arg = True # making it so we write header only once
chunk_count = 0
appended_sum = 0
with pd.read_csv(INP_FILENAME, chunksize=CHUNK, sep=';',
                 parse_dates=['ts']) as reader:
    for chunk in reader:
        df = chunk.loc[(chunk['ts'] >= DT_BEG) & (chunk['ts'] < DT_END)]
        df.to_csv(f'{out_fname}', mode='a', sep=';', index=False,
                  header=header_arg)
        header_arg = False
        chunk_count += 1
        appended_sum += len(df.index)
        sys.stdout.write(f'\r{" "*100}\r')
        sys.stdout.flush()
        sys.stdout.write(f'Chunk #{chunk_count: } completed (running total: '
              + f'{chunk_count*CHUNK:,} rows read, {appended_sum:,} rows appended.)')
        sys.stdout.flush()

end_time = time.time()
print('\nDone. Elapsed time:',
      time.strftime("%H:%M:%S", time.gmtime(end_time-start_time)))
