import pandas as pd
import numpy as np
import copy as cp
import math
import rpy2.robjects as robjects
import rpy2.robjects.numpy2ri
import time
from multiprocessing import Pool
from multiprocessing import set_start_method
from scipy.stats import lognorm
from scipy.stats import burr
from scipy.stats import kstest
from scipy.interpolate import interp1d
from concurrent.futures import ThreadPoolExecutor
from statsmodels.distributions.empirical_distribution import ECDF

r = robjects.r
robjects.numpy2ri.activate()
r.source('lnormpar.R')

def empirical_sample(size, ecdf):
    inv_cdf = interp1d(ecdf.y, ecdf.x, bounds_error = False, assume_sorted=True)
    r = np.random.uniform(0, 1, size)
    return inv_cdf(r)

def mle_lognorm(minimum, maximum, mean, size):
    estimate = r.lnormpar(x1=minimum,xn=maximum,xbar=mean,n=size)
    return estimate[0], estimate[1], estimate[2], estimate[3]

def set_timestamps(dataframe, duration, interval):
    nr_entries = round(duration / interval)
    time_increment = 0
    for i in range(nr_entries):
        time_increment += interval
        new_row = {'Timestamp [ms]':time_increment,
                   'Invocations':0,
                   'Exec_time':0,
                   'Provisioned_cpu':0,'Provisioned_memory':0,
                   'Avg_cpu':0,
                   'Avg_mem':0}
        dataframe = dataframe.append(new_row, ignore_index=True)
    return dataframe

def set_provisioning(dataframe, prov_cpu, prov_mem):
    dataframe = dataframe.assign(Provisioned_cpu=prov_cpu)
    return dataframe.assign(Provisioned_memory=prov_mem)

non_zero = lambda t: t if t > 0 else 0

trace_template = {'Timestamp [ms]':[],
                  'Invocations':[],
                  'Exec_time':[],
                  'Provisioned_cpu':[],'Provisioned_memory':[],
                  'Avg_cpu':[],
                  'Avg_mem':[]}
trace_dtypes = {'Timestamp [ms]':int,
                  'Invocations':int,
                  'Exec_time':int,
                  'Provisioned_cpu':int,'Provisioned_memory':int,
                  'Avg_cpu':float,
                  'Avg_mem':float}


def gen_trace(function, name):
	try:
		# Estimating parameters for the execution times lognormal distribution 
		mu, sigma, mu_error, sigma_error = mle_lognorm(minimum=function[1446]+1,
				                                       maximum=function[1447]+1,
				                                       mean=function[1444]+1,
				                                       size=function[1445]+1)
		exec_dist = lognorm(s=sigma, scale=math.exp(mu))
	except rpy2.rinterface_lib.embedded.RRuntimeError:	
		print("Problem")

	# Generating invocations CDF
	invoc_sample = function[4:1444]

	# Generating an OpenDC serverless trace
	df = pd.DataFrame(data=trace_template).astype(trace_dtypes)

	df = set_timestamps(dataframe=df, duration=86400000, interval=60000)

	df = set_provisioning(dataframe=df,prov_cpu=100,prov_mem=128)

	df = df.assign(Invocations=invoc_sample).astype(np.int)
	
	df = df.assign(Exec_time=exec_dist.rvs(1440)).astype(np.int)

	# Assuming uniformity of allocated memory between functions of the same application
	df = df.assign(Avg_mem=function[1456]).astype(np.int)

	df = df.rename(columns={'Exec_time':' Avg Exec time per Invocation',
	                    'Provisioned_cpu':'Provisioned CPU [Mhz]',
	                    'Provisioned_memory':'Provisioned Memory [mb]',	
	                    'Avg_cpu':' Avg cpu usage per Invocation [Mhz]',
	                    'Avg_mem':' Avg mem usage per Invocation [mb]'})

	df.to_csv(f'/home/soufianej/Documents/Bachelors_project/serverless-simulator/serverless/traces/Lambda/{name}.csv',index=False)



def generate_discrete_traces(slice_index):
	functions_df = pd.read_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/tasks/task{0}.csv'.format(slice_index), index_col=False)	
	hashapp_dict = pd.read_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/apps.csv', index_col=False).set_index('HashApp')['Count'].to_dict()


	for function in functions:	
		try:
			# Estimating parameters for the execution times lognormal distribution 
			mu, sigma, mu_error, sigma_error = mle_lognorm(minimum=function[1446]+1,
					                                       maximum=function[1447]+1,
					                                       mean=function[1444]+1,
					                                       size=function[1445]+1)
			exec_dist = lognorm(s=sigma, scale=math.exp(mu))

			# Estimating parameters for the memory allocation lognormal distribution 
			mu, sigma, mu_error, sigma_error = mle_lognorm(minimum=function[1457]+1,
									   					   maximum=function[1464]+1,
					                                       mean=function[1456]+1,
					                                       size=function[1455]+1)
			mem_dist = lognorm(s=sigma, scale=math.exp(mu))
		except rpy2.rinterface_lib.embedded.RRuntimeError:	
			continue


		# Generating invocations CDF
		invoc_sample = function[4:1444]
		invoc_dist = ECDF(invoc_sample)

		# Generating an OpenDC serverless trace
		df = pd.DataFrame(data=trace_template).astype(trace_dtypes)

		df = set_timestamps(dataframe=df, duration=86400000, interval=60000)

		df = set_provisioning(dataframe=df,prov_cpu=100,prov_mem=256)

		df = df.assign(Invocations=np.array([non_zero(invoc) for invoc in empirical_sample(size=1440,ecdf=invoc_dist)])).astype(np.int)
		
		df = df.assign(Exec_time=exec_dist.rvs(1440)).astype(np.int)

		# Assuming uniformity of allocated memory between functions of the same application
		nr_functions = hashapp_dict.get(function[1])
		df = df.assign(Avg_mem=np.divide(mem_dist.rvs(1440), nr_functions)).astype(np.int)

		df = df.rename(columns={'Exec_time':' Avg Exec time per Invocation',
		                    'Provisioned_cpu':'Provisioned CPU [Mhz]',
		                    'Provisioned_memory':'Provisioned Memory [mb]',	
		                    'Avg_cpu':' Avg cpu usage per Invocation [Mhz]',
		                    'Avg_mem':' Avg mem usage per Invocation [mb]'})
		df.to_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/converted/{0}({1}).csv'.format(function[2],function[1465]),index=False)

	return f'Slice {slice_index}: Done'


def generate_continuous_traces(slice_index):
	all_functions = pd.read_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/tasks/task{0}.csv'.format(slice_index), index_col=False)
	hashapps = pd.read_csv(r'/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/apps.csv', index_col=False)
	hashapp_dict = hashapps.set_index('HashApp')['Count'].to_dict()
	
	lookup_df = all_functions.groupby('HashFunction').count()
	hashapps['indices'] = ""

	for index, func_key in enumerate(lookup_df.iterrows(), start=1):
		functions = all_functions[all_functions['HashFunction'] == func_key[0]]

		df = pd.DataFrame(data=trace_template).astype(trace_dtypes)
		df = set_timestamps(dataframe=df, duration=604800000, interval=60000)
		df = set_provisioning(dataframe=df,prov_cpu=100,prov_mem=256)
		df = df.assign(name=func_key[0])
		name = -1

		start = 0
		end = 1439

		hashapp = ""

		for i in range(7):
			day = functions[functions['Day'] == i+1].values
			if day.size != 0:
				name = day[0][1466]
				hashapp = day[0][1]

				invoc_sample = day[0][4:1444]
				invoc_dist = ECDF(invoc_sample)

				nr_functions = hashapp_dict.get(day[0][1])

				try:
					# Estimating parameters for the execution times lognormal distribution 
					mu, sigma, mu_error, sigma_error = mle_lognorm(minimum=day[0][1446]+1,
									               maximum=day[0][1447]+1,
									               mean=day[0][1444]+1,
									               size=day[0][1445]+1)
					exec_dist = lognorm(s=sigma, scale=math.exp(mu))

					# Estimating parameters for the memory allocation lognormal distribution 
					mu, sigma, mu_error, sigma_error = mle_lognorm(minimum=day[0][1457]+1,
										       maximum=day[0][1464]+1,
									               mean=day[0][1456]+1,
									               size=day[0][1455]+1)
					mem_dist = lognorm(s=sigma, scale=math.exp(mu))
				except rpy2.rinterface_lib.embedded.RRuntimeError:
				    if (day[0][3] == "timer"):
				        df.loc[start:end, 'Invocations'] = invoc_sample
				    else:
					    df.loc[start:end, 'Invocations'] = np.array([non_zero(invoc) for invoc in empirical_sample(size=1440,ecdf=invoc_dist)]).astype(np.int)
				    df.loc[start:end, 'Exec_time'] = day[0][1451]
				    df.loc[start:end, 'Avg_mem'] = np.divide(day[0][1456], nr_functions).astype(np.int)
				    continue
				if (day[0][3] == "timer"):
				    df.loc[start:end, 'Invocations'] = invoc_sample
				else:
				    df.loc[start:end, 'Invocations'] = np.array([non_zero(invoc) for invoc in empirical_sample(size=1440,ecdf=invoc_dist)]).astype(np.int)
				df.loc[start:end, 'Exec_time'] = exec_dist.rvs(1440).astype(np.int)
				df.loc[start:end, 'Avg_mem'] = np.divide(mem_dist.rvs(1440), nr_functions).astype(np.int)
			else:
				df.loc[start:end, 'Invocations'] = 0
				df.loc[start:end, 'Exec_time'] = 0
				df.loc[start:end, 'Avg_mem'] = 0

			start += 1440
			end += 1440
	
		df = df.rename(columns={'Exec_time':' Avg Exec time per Invocation',
											'Provisioned_cpu':'Provisioned CPU [Mhz]',
		                    				'Provisioned_memory':'Provisioned Memory [mb]',	
		                    				'Avg_cpu':' Avg cpu usage per Invocation [Mhz]',
		                    				'Avg_mem':' Avg mem usage per Invocation [mb]'})

		if name != -1:
		    old_indices = hashapps.loc[hashapps['HashApp'] == hashapp, 'indices']
		    new_indices = old_indices + f" {name}"
		    hashapps.loc[hashapps['HashApp'] == hashapp, 'indices'] = new_indices

		df.to_csv(f"/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/converted/{name}.csv", index=False)

		
	hashapps.to_csv(f"/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/indices{slice_index}.csv", index=False)



def run_tasks():
	start_time = time.time()
	print(f"Starting process pool at time: {start_time}")	
	pool = Pool()

	slices = [0,1,2,3,4,5,6,7]

	pool.map(generate_continuous_traces, slices)
	
	pool.close()
	pool.join()

	end_time = time.time() - start_time
	print(f"it took {end_time} to finish all tasks")








