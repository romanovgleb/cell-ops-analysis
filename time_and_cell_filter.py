'''
Script takes in cellular operators' travel matrix and multiple territories'
cell lists, reads matrix in chunks, filters rows for specified date and time
range, and aggregates trips from and to each of the territories specified in
the list (given that either origin or destination is in the zone's cell list),
then writes the output.
'''

# read in the cell id files
# create zones lists
# iterate over large cell csv
# set the day to be 2019.10.16
# write _from and _to data to each file accordingly

import pandas as pd
import sys # clearing print statements
import time
import os  # approximate rowsize count based on file size

start_time = time.time() # measuring elapsed time

INP_FILENAME = 'CMatrix.csv'
CHUNK = 10 ** 6                                # amount of rows in read chunk
DT_BEG = pd.to_datetime('2021-10-16 00:00:00') # date & time we want to include
DT_END = pd.to_datetime('2021-10-17 00:00:00')
OUT_FILENAME = 'expo'
INCLUDE_TIMESTAMPS = True

cell_file_list = ['east', 'west', 'center']
cell_var_list = []
for cfile in cell_file_list:
    globals()[cfile] = pd.read_excel('cell_list_'+cfile+'.xlsx')
    cell_var_list.append(globals()[cfile])
total = pd.concat(cell_var_list)
cell_file_list.append('total')
cell_var_list.append(total)

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
      +f'{DT_BEG} and {DT_END}\nand appending to "{out_fname}"\n')

# reading file by chunks, filtering dates, appending to csv
chunk_count = 0
appended_sum = 0
with pd.read_csv(INP_FILENAME, chunksize=CHUNK, sep=';',
                 parse_dates=[0], header=None) as reader:
    for chunk in reader:
        df = chunk.loc[(chunk[0] >= DT_BEG) & (chunk[0] < DT_END)]
        i = 0
        for cvar in cell_var_list:
            cvar_name = cell_file_list[i]
            df_from = df.loc[df[1].isin(cvar.cell_id)]
            df_to = df.loc[df[2].isin(cvar.cell_id)]
            # writing result
            df_from.to_csv(f'{OUT_FILENAME}_from_{cvar_name}.csv', mode='a',
                           sep=';', index=False, header=False)
            df_to.to_csv(f'{OUT_FILENAME}_to_{cvar_name}.csv', mode='a',
                           sep=';', index=False, header=False)
            appended_sum += len(df_from.index)
            appended_sum += len(df_to.index)
            i += 1
        chunk_count += 1
        sys.stdout.write(f'\r{" "*100}\r')
        sys.stdout.flush()
        sys.stdout.write(f'Chunk #{chunk_count: } completed (running total: '
              + f'{chunk_count*CHUNK:,} rows read, {appended_sum:,} rows appended.)')
        sys.stdout.flush()

end_time = time.time()
print('\nDone. Elapsed time:',
      time.strftime("%H:%M:%S", time.gmtime(end_time-start_time)))
