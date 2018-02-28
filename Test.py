import dask
import os
import zipfile
import datetime


@dask.delayed
def read_1file(file_name):
    with open(file_name, 'r') as x:
        i = 0
        for y in x:
            i += 1
        return i


# @dask.delayed
def read_file(zip, file_name):
    x = zip.read(file_name)
    i = 0
    for y in x:
        i += 1
    return i


def get_file_list(compressedfile):
    archive = zipfile.ZipFile(compressedfile, 'r')
    print(archive, type(archive))
    count = []
    for f in archive.filelist:
        assert isinstance(f, zipfile.ZipInfo)
        # print(f,type(f),f.filename)
        count.append(read_file(archive, f))
    return count


def oswalk2(f):
    import os
    for root, dirs, files in os.walk(".", topdown=False):
        for name in files:
            print(os.path.join(root, name))
        for name in dirs:
            print(os.path.join(root, name))
    return 1


def oswalk(f):
    import os

    file_list = []
    print("in")
    for root, dirs, files in os.walk(f, topdown=False):
        for name in files:
            print(os.path.join(root, name))
            file_list.append(os.path.join(root, name))
        for name in dirs:
            pass
            print(os.path.join(root, name))
    return file_list


def process_files(file_list):
    lines = []

    for f in file_list:
        lines.append(pandas_read(f))
        #print("-----", f)
    return lines

#@dask.delayed
def pandas_read(filename):
    import pandas as pd
    chunksize = 10 ** 6
    total_lines=0
    print("processing file:",filename)
    #for chunk in pd.read_csv(filename, chunksize=chunksize):
     #   total_lines+=len(chunk)
    df = pd.read_csv(filename)

    #print(list(df))
    #print(df['SF1ST'])
    #df.groupby(df.SF1ST)
    #return total_lines
    return len(df)

def dask_panda(file):
    import dask.dataframe as dd
    import pandas as pd
    df = dd.read_csv(file)
    print(dir(df))
    # df.groupby(df.).value.mean().compute()


zip_file = ['/Users/hung/Downloads/aff_download.zip'
    , '/Users/hung/Downloads/ca2010.sf1.zip'
    , '/Users/hung/Downloads/dc2010.sf1.zip']
print(os.path.basename(zip_file[1]))

s1 = datetime.datetime.now()

#dask_panda('/Users/hung/Downloads/ca2010.sf1/ca000252010.sf1')
#pandas_read('/Users/hung/Downloads/ca2010.sf1/ca000252010.sf1')

fl=oswalk('/Users/hung/Downloads/ca2010.sf1/')
# process_files(fl)
# count=get_file_list(zip_file[1])
# print(sum(count))

lines=process_files(fl)
print("",sum(lines))
#total = dask.delayed(sum)(lines)
# total.visualize()
#print("Total",total.compute())
# print("Total lines Processed") #18938557
# print("total lines",(process_files(fl)))


s2 = datetime.datetime.now()
print("Total Time:", s2 - s1)

"""('Total', 18094806)
('Total Time:', datetime.timedelta(0, 186, 111477))"""
