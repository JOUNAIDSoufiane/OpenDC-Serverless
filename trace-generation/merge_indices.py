import pandas as pd
from functools import reduce

indices = []

for i in range(8):
	indices.append(pd.read_csv(f"/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/indices{i}.csv"))

df = reduce(lambda left,right: pd.merge(left,right, on=['HashApp','Count','popularity']),  indices)

df.to_csv("/media/soufianej/Transcend/Traces/Azure/OpenDCServerless/Samples/sample1/indices.csv", index=False) 	
