import pandas as pd
import numpy as np
import copy as cp
import math

def compute_functions(index):
    invocations = pd.read_csv(f"/media/soufianej/Transcend/Traces/Azure/invocations/invocations_per_function_md.anon.d0{index}.csv", index_col=False)
    exec_times  = pd.read_csv(f"/media/soufianej/Transcend/Traces/Azure/duration/function_durations_percentiles.anon.d0{index}.csv", index_col=False)
    memory_alloc= pd.read_csv(f"/media/soufianej/Transcend/Traces/Azure/memory/app_memory_percentiles.anon.d0{index}.csv", index_col=False)

    # Merging invocations, execution time and memory alloc into a signle dataframe

    invoc_exec_merged = pd.merge(invocations, exec_times, on=["HashOwner","HashApp","HashFunction"])
    functions = pd.merge(invoc_exec_merged, memory_alloc, on=["HashOwner","HashApp"], how="left")
    functions.reset_index(inplace=True,drop=True)

    # Filtering out unwanted duplicates
    try:
        unwanted = pd.concat(g for _, g in functions.groupby("HashFunction") if len(g) > 1).sort_values(by="HashFunction")

        for row in unwanted.HashFunction:
            if (row in unwanted.HashFunction.values):
                functions = functions[functions.HashFunction != row]
        functions.reset_index(inplace=True,drop=True)
    except ValueError:
        print("no duplicates")

    functions.to_csv(f'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Functions/FunctionsDay{index}.csv', index=False)
    return functions

#Filteting out mostly idle functions (low invocation frequency)

def filter_functions(functions, threshold, inverse):
    to_drop = []

    for index,row in enumerate(functions.values, start=0):
        count = 0

        for i in range(1440):
            if (row[i+4] != 0):
                count += 1
        
        if (count / 1440 > threshold):
            if inverse is True:
                to_drop.append(index)
            else:
                continue
        else:
            if inverse is True:
                continue
            else:
                to_drop.append(index)

    functions.drop(to_drop, axis='index', inplace=True)
    functions.reset_index(drop=True, inplace=True)


# Splitting the task into smaller tasks than can be executed in parallel
def split_tasks(functions):
    lookup_df = functions.groupby('HashApp').count()

    apps = []
    for i in lookup_df.iterrows():
	    apps.append(i[0])
    size = len(apps)
    sample = math.ceil(size / 8) # task size divided by number of threads

    start = 0
    end = sample

    for i in range(8):
        if (end > size-1):
            end = size
        print('functions split: start = {0}, end = {1}'.format(start, end))
        print(apps[start:end])
        functions[functions['HashApp'].isin(apps[start:end])].to_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/tasks/task{0}.csv'.format(i), index=False)
        start += sample
        end += sample
 

def compute_popularity(functions):
    popularity_series = functions.groupby('HashApp').HashApp.count()
    popularity_df = popularity_series.to_frame()
    popularity_df.assign(popularity = 0)

    popularity_df = popularity_df.rename(columns={'HashApp':'Count'})
    popularity_set = np.array([])

    for app_hash in popularity_series.keys():
        app_functions = functions.loc[functions['HashApp'] == app_hash]
        popularity_index = app_functions.apply(lambda row: row['1' : '1440'].sum(),axis=1).sum()
        popularity_set = np.append(popularity_set, popularity_index)


    popularity_df = popularity_df.assign(popularity = popularity_set)

    popularity_df.to_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/HashApps/HashAppsDay7.csv')


def sample_apps(n,hash_apps,seed):
    return hash_apps.loc[((hash_apps['popularity'] > 1500) & (hash_apps['popularity'] < 2500))].sample(n=n, random_state=np.random.RandomState(seed), axis=0)

def get_functions(functions, hash_apps):
    sample = functions.loc[functions['HashApp'].isin(hash_apps.HashApp)]
    sample = sample.assign(id=0)
    # Setting indexes
    index_df = sample.groupby('HashFunction').count()
    for index, func_key in enumerate(index_df.iterrows(), start=1):
        sample.loc[sample['HashFunction'] == func_key[0], 'id'] = index
    return sample




















