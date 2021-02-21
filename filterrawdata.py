'''
The IMDb data files that will initially be used for this project are as follows:
1. title.basics.tsv
2. title.akas.tsv
3. title.rating.tzv

Both the title.basics and title.akas files are too large to be uploaded to github.  For these reasons the files will be pre-filtered
through this filterrawdata.py file first.  The tsv files are located in the 
rawdata subdirectory.  Newly created filtered files will be placed in the 
filteredrawdata subdirectory using the same format for subsequent Data Wrangling
and cleaning.

The data dictionary for the tsv files can be found in the subdirectory datadict

The goal of this project is to identify what makes a highly rated MOVIE in the US using data from 1990-present.  These are the 
initial 3 filters that we will attempt to apply in this prefiltering exercise.
'''
#%%
# import numpy and pandas
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.core.algorithms import value_counts
from pathlib import Path

#%%
# First we'll investigate the the primary file, title.basics.tsv.  Let's see the current file size.
print(Path('rawdata/title.basics.tsv').stat())

# os.stat_result(st_mode=33261, st_ino=27364, st_dev=2064, st_nlink=1, st_uid=1000, st_gid=1000, st_size=649028657, st_atime=1613931772, st_mtime=1613786375, st_ctime=1613786375)
#%%
# use pandas to import the primary file (title.basics.tsv) one chunksize = 5000 to evaluate the data.  My initial thoughts on
# this prefiltering exercise is to simply filter on titleType = 'movies'  
basics_reader = pd.read_csv('rawdata/title.basics.tsv', sep='\t', chunksize=5000)
df_basics = next(basics_reader)

print(df_basics.head())

#%%
print(df_basics['titleType'].describe())

#%%
print(df_basics['titleType'].value_counts())

# the first chunk (5000 records) only contain 2 titleTypes (short and movie) and movie is < 20% (947 records).  
# %%
# Based on the results of the first 5000 records, I am going to loop over the entire file using a generator and store the
# titleType and counts in a dictionary to evaluate and create a bar chart
# Based on the data dictionary we know there there are a limited of titleTypes 

# create generator function
def read_large_file(file_obj):
    '''A generator function to read large files lazily.'''

    # Loop indefitely until EOF
    while True:
        line = file_obj.readline()

        if not line:
            break

        yield line

#%%
titleTypeDict = {}

with open('rawdata/title.basics.tsv') as file:
    
    # skip the header record
    file.readline()

    # use generator to readlines and populate dictionary
    for line in read_large_file(file):
        row = line.split('\t')
        titleType = row[1]

        if titleType in titleTypeDict.keys():
            titleTypeDict[titleType] += 1
        else:
            titleTypeDict[titleType] = 1

print(titleTypeDict)
# {'short': 792881, 'movie': 567649, 'tvShort': 9588, 'tvMovie': 129888, 'tvSeries': 201161, 'tvEpisode': 5513333, 'tvMiniSeries': 35756, 'tvSpecial': 31369, 'video': 294787, 'videoGame': 27312, 'audiobook': 1, 'radioSeries': 1, 'episode': 1}

#%%
plt.bar(titleTypeDict.keys(), titleTypeDict.values())
plt.xticks(rotation=45)
plt.savefig('images/rawTitleType.png')
plt.show()
# the results show
# %%
# Now we see that movies make up a small percentage of the records.  We'll write the movie records to a new file and review the new file size.

title_basics_out = open('filterdata/title.basics.tsv', 'a')

with open('rawdata/title.basics.tsv') as file:
    
    # write the header record
    header = file.readline()
    title_basics_out.write(header)

    # use generator to readlines, check if titleType = movie and write to new file
    for line in read_large_file(file):
        row = line.split('\t')
        titleType = row[1]

        if titleType == 'movie':
            title_basics_out.write(line)

title_basics_out.close()


# %%
print(Path('filterdata/title.basics.tsv').stat())
# os.stat_result(st_mode=33188, st_ino=2208, st_dev=2064, st_nlink=1, st_uid=1000, st_gid=1000, st_size=43873056, st_atime=1613940804, st_mtime=1613940813, st_ctime=1613940813)
# this file is considerably smaller and will be uploadable to github at ~43MB
# %%
# We will now perform the same checks we did on the intitial file
basics_reader = pd.read_csv('filterdata/title.basics.tsv', sep='\t', chunksize=5000)
df_basics = next(basics_reader)

print(df_basics.head())

#%%
print(df_basics['titleType'].describe())

#%%
print(df_basics['titleType'].value_counts())
# %%
'''
The 2nd file we need to evaluate due to it's large file size is title.akas.tsv.  Unfortunately this file
does not contain titleTypes as in the first file, however it does contain "region".  Based on our requirements
we do only want to filter for US Movies.  We'll follow similar steps as we did for the title.basics file to review 
some details and perform filtering
'''
# First we'll investigate the the primary file, title.basics.tsv.  Let's see the current file size.
print(Path('rawdata/title.akas.tsv').stat())

# os.stat_result(st_mode=33261, st_ino=27300, st_dev=2064, st_nlink=1, st_uid=1000, st_gid=1000, st_size=1244389027, st_atime=1613941261, st_mtime=1613786369, st_ctime=1613786369)

#%%
# use pandas to import the secondary file (title.akas.tsv) one chunksize = 5000 to evaluate the data.  My initial thoughts on
# this prefiltering exercise is to simply filter on region = 'US'  
akas_reader = pd.read_csv('rawdata/title.akas.tsv', sep='\t', chunksize=5000)
df_akas = next(akas_reader)

print(df_akas.head())

#%%
print(df_akas['region'].describe())
# Bad news, US is the top region with 1510 of the 5000
#%%
print(df_akas['region'].value_counts())

# the first chunk (5000 records) only contain numerous regions with US at the top of the list.  We may have to do additional 
# cleaning to get this file down to size that can be uploaded to github  

# %%
# This file is much larger than the basic file and it seems that what we want to filter on (region) may have most values of US
# which are the values we want to keep.  We'll use a modified version of the dictionary we used previously to get actual % values

regionDict = {}

with open('rawdata/title.akas.tsv') as file:
    
    # skip the header record
    file.readline()

    # use generator to readlines and populate dictionary
    for line in read_large_file(file):
        row = line.split('\t')
        region = row[3]

        if region != 'US':
            region = 'Other'

        if region in regionDict.keys():
            regionDict[region] += 1
        else:
            regionDict[region] = 1

print(regionDict)
# {'Other': 23990937, 'US': 1164759}
# These are encounraging results
 # %%
#%%
plt.bar(regionDict.keys(), regionDict.values())
plt.xticks(rotation=45)
plt.savefig('images/rawAkas.png')
plt.show()
# %%
print(100 * regionDict['US']/(regionDict['US'] + regionDict['Other']))
# only 4.63% of the records are 'US', this should dramtically decrease the file size.
# %%
# As with the title.basics file, we are going to read the title.akas file and create a filtered version for region = US only

title_akas_out = open('filterdata/title.akas.tsv', 'a')

with open('rawdata/title.akas.tsv') as file:
    
    # write the header record
    header = file.readline()
    title_akas_out.write(header)

    # use generator to readlines, check if titleType = movie and write to new file
    for line in read_large_file(file):
        row = line.split('\t')
        region = row[3]

        if region == 'US':
            title_akas_out.write(line)

title_akas_out.close()
# %%
print(Path('filterdata/title.akas.tsv').stat())

# os.stat_result(st_mode=33188, st_ino=2863, st_dev=2064, st_nlink=1, st_uid=1000, st_gid=1000, st_size=56404414, st_atime=1613943742, st_mtime=1613943771, st_ctime=1613943771)
# %%
'''
The 3rd file title.ratings.tsv is not too large.  Additionally there is no filtering based on the data that can be
performed initially.  We'll just take a quick look at the the filesize to complete this initial exercise
'''
# First we'll investigate the the primary file, title.basics.tsv.  Let's see the current file size.
print(Path('rawdata/title.ratings.tsv').stat())

# os.stat_result(st_mode=33261, st_ino=29916, st_dev=2064, st_nlink=1, st_uid=1000, st_gid=1000, st_size=19153091, st_atime=1613943972, st_mtime=1613786388, st_ctime=1613786388)
