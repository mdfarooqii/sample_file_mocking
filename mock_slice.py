import pandas as pd 
import numpy as np 
import sys 
if len(sys.argv) != 4: 
	print(' Usage: python ' + sys.argv[0] + ' pos_count rv_count mds_count ') sys.exit(-1) 
position_file_row_count, risk_file_row_count, mds_file_row_count = sys.argv[1:] 
position_file_row_count=int(position_file_row_count) 
risk_file_row_count=int(risk_file_row_count) 
mds_file_row_count=int(mds_file_row_count) 
# position_file_row_count = 100000 
# risk_file_row_count = 1000000 
# mds_file_row_count = 100000 
current_date = pd.Timestamp('today').strftime('%m/%d/%Y') 
dfLinguaMap = pd.read_csv(r'/home/ariskdev/dattanvi/py/lingua_file_column_mapping.csv') 
dfPosMap = dfLinguaMap[dfLinguaMap['FILE_TYPE_NAME'] == 'POSITION'] 
dfMdsMap = dfLinguaMap[dfLinguaMap['FILE_TYPE_NAME'] == 'MDS'] 
dfRvMap = dfLinguaMap[dfLinguaMap['FILE_TYPE_NAME'] == 'RISK_VALUE'] 

def generate_random_string(name, count): 
	listStr = [name+str(i) for i in list(range(count))] 
	return listStr 

def generate_random_int(name, count): 
	listInt = [i for i in np.random.randint(1000,10000,size=count)] 
	return listInt 
def generate_date(name, count): 
	listDte = [current_date] * count 
	return listDte 
	
def assign_function (action): 
	switcher = { 
					'generate_random_string':generate_random_string, 
					'generate_random_int':generate_random_int, 
					'generate_date':generate_date } 
	return switcher.get(action,generate_random_string)

position_schema_dict = dfPosMap.set_index('COLUMN_NAME')['PY_FUNCTION_CALL'].to_dict() 
position_func_dict={x:assign_function(position_schema_dict[x]) for x in list(position_schema_dict.keys())} 
position_dict = {x:position_func_dict[x](x,position_file_row_count) for x in list(position_func_dict.keys())} 
mds_schema_dict = dfMdsMap.set_index('COLUMN_NAME')['PY_FUNCTION_CALL'].to_dict() 

mds_func_dict={x:assign_function(mds_schema_dict[x]) for x in list(mds_schema_dict.keys())} 
mds_dict = {x:mds_func_dict[x](x,mds_file_row_count) for x in list(mds_func_dict.keys())} 

def reuse_position_id (name, count): 
	listPos = position_dict['id']* int(risk_file_row_count/position_file_row_count) 
	return listPos 

def reuse_mds_id (name, count): 
	listMds = mds_dict['id']* int(risk_file_row_count/mds_file_row_count) 
	return listMds 

def assign_reuse_function (action): 
	switcher = { 
					'generate_random_string':generate_random_string, 
					'generate_random_int':generate_random_int, 
					'generate_date':generate_date, 
					'reuse_position_id':reuse_position_id, 
					'reuse_mds_id':reuse_mds_id 
	} 
	return switcher.get(action,generate_random_string) 

risk_value_schema_dict = dfRvMap.set_index('COLUMN_NAME')['PY_FUNCTION_CALL'].to_dict() 
risk_value_func_dict={x:assign_reuse_function(risk_value_schema_dict[x]) for x in list(risk_value_schema_dict.keys())}
risk_value_dict = {x:risk_value_func_dict[x](x,risk_file_row_count) for x in list(risk_value_func_dict.keys())} 
df_position = pd.DataFrame(position_dict) 
df_mds = pd.DataFrame(mds_dict) 
df_risk_value = pd.DataFrame(risk_value_dict) 

#print ('Position file:') 
#print (df_position) 
df_position.to_csv('position.txt', sep = ' ', index=False) 
#print ('Mds file:') 
#print (df_mds)
df_mds.to_csv('mds.txt', sep = ' ', index=False) 
#print ('Risk value file:') 
#print (df_risk_value) 
df_risk_value.to_csv('rv.txt', sep = ' ', index=False)