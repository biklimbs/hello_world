##---This program takes the data which are not present in the "history_cleaned" and stores it in "tmp" table for cleaning---

#---Importing packages---
import sys
sys.path.append('/usr/local/lib/python3.5/dist-packages')
import pymysql.cursors
import pandas as pd
import re
from logger_config import *
import logger_config
import time as t
from datetime import datetime,date,timedelta
import warnings
warnings.filterwarnings("ignore")
from google.cloud import translate
# Instantiates a client
#translate_client = translate.Client()

translate_client=translate.Client.from_service_account_json('Japanase-To-English-ee8f40422467.json')



#---Configuring log filename---
log_file=os.path.splitext(os.path.basename(__file__))[0]+".log"
log = logger_config.configure_logger('default', ""+DIR+""+LOG_DIR+"/"+log_file+"")


__author__="Bikash Limboo"
__maintainer__="Bikash Limboo"


#---Api count file---

API_COUNT_FILE="api_call_count.txt"

#---Server credentials---
HOST= "spark.vmokshagroup.com"
USER="root"
PASSWORD="Vmmobility@1234"

#---Mysql database details---
SOURCE_DATABASE='cawm_rawdata'
TRANSLATION_DATABASE='cawm_translate'
EXCEPTION_DATABASE='cawm_exception'

#TARGET_TABLE="history_cleaned"
SOURCE_TABLE="history"
TMP_TABLE="tmp_1"

TRANSLATION_TABLE_NAME="grade_lookup"
COLUMN_NAME="GRADE"

#---Column names for searching japanese hex code---
HEX_CODE_COLUMN=['GRADE','KPP','KUZOV','COLOR','RATE']

#---SQL queries---
SQL_QUOTE="\'"
SQL_NEW_DATA_FETCHING="select * from "
SQL_WHERE_CLEAN_FLAG=" where clean_flag=0 and AUCTION not like 'USS%%' and FINISH!=0 and STATUS in('SOLD','SOLD BY NEGO')"
SQL_UPDATE_CLEAN_FLAG='update history,tmp_1 set history.clean_flag=1 where history.ID=tmp_1.ID'


#---Connection to "DATABASE_cawm_rawdata"---
def connect_to_db(database_name):
	connection = pymysql.connect(host=HOST,
                                     user=USER,
                                     password=PASSWORD,
                                     db=database_name,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
	return connection



#---Read the data from any table in the db---
def read_data_from_db(connection,sql_query):
	with connection.cursor() as cursor:
		try:
			sql=sql_query
			cursor.execute(sql)
			if cursor.rowcount > 0:
				df=pd.DataFrame(cursor.fetchall())
				return df
			else:
				print("Empty Results")
			
		except Exception as e:
			log.error(e)


#---Separate japanese hex code into df---
def segregate_rows_in_japanese(df_normal,column_name):
	df_hex=df_normal[df_normal[column_name].str.contains('.*(&#).*',case=False)]
	df_normal=df_normal[df_normal[column_name].str.contains('.*(&#).*',case=False)==False]
	return df_hex,df_normal

#---Compare color against the color code present in the db---
def compare_hexcode_from_db(df,df_trans,column_name):
	df_merge=pd.merge(df,df_trans,on=column_name, how='inner')
	df_merge=df_merge.drop_duplicates() 
	return df_merge


#---Translate the japanese hex code---
def preprocess_history_data(df_normal):
	#---Separating japanese_hexcode and normal data---
	for column_name in HEX_CODE_COLUMN:
		df_hex,df_normal=segregate_rows_in_japanese(df_normal,column_name)

		###---cleaning code here---
		lookup_table=column_name.lower()+"_lookup"
		translation_db_connection=connect_to_db(TRANSLATION_DATABASE)
		df_trans=read_data_from_db(translation_db_connection,SQL_NEW_DATA_FETCHING+lookup_table)	
		df_japan_to_english=compare_hexcode_from_db(df_hex,df_trans,column_name)
		print()
		df_new=pd.merge(df_hex,df_japan_to_english[['ID',column_name.lower()+"_english"]],on="ID",how="left")
		#df_new.to_csv(column_name+".csv",index=False)
		df_normal=df_normal.append(df_new,ignore_index=True)

	return df_normal


#---Remove all the values from the columns---	
def remove_null_values(df):
	df.grade_english.fillna(df["GRADE"],inplace=True)
	df.kpp_english.fillna(df["KPP"],inplace=True)
	df.kuzov_english.fillna(df["KUZOV"],inplace=True)
	df.rate_english.fillna(df["RATE"],inplace=True)
	df.color_english.fillna(df["COLOR"],inplace=True)

	df.drop(columns=['GRADE','KPP','KUZOV','COLOR','RATE'],inplace=True)
	df.rename(columns={'grade_english':'GRADE','kpp_english':'KPP','kuzov_english':'KUZOV','color_english':'COLOR','rate_english':'RATE'},inplace=True)
	return df



#---Updating lookup table---
def insert_into_lookup(column_name,hex_value,translated_value):
	connection=connect_to_db(TRANSLATION_DATABASE)
	table_name=column_name.lower()+"_lookup"
	with connection.cursor() as cursor:
		try:			
			sql = "INSERT INTO "+table_name+" values("+"\""+hex_value+"\","+"\""+str(translated_value)+"\"" + ")"
			#print(sql)
			cursor.execute(sql)
			connection.commit()
			log.info("Updating ")
		except Exception as e:
			log.error(e)

#---This will read the previous total api count---
def read_api_count():
	f_read=open(API_COUNT_FILE,mode="r",encoding='utf-8')
	num=f_read.read()
	f_read.close()
	return num

#---This will write the latest total api count in file---	
def write_api_count(num):
	api_call_count=open(API_COUNT_FILE,mode="w+",encoding='utf-8')
	num=str(int(num)+1)
	api_call_count.write(num)
	api_call_count.close()	


#---Calling google translate api if data is not present in lookup table---
def translate_api_call(df,column_name):
	hex_column=column_name+"_hex"
	df[hex_column]=df[column_name]
	for index,row in df.iterrows():
		try:
			translation = translate_client.translate(str(df.at[index,column_name]),target_language='en')
			api_call_count=read_api_count()
			write_api_count(api_call_count)
			df.at[index,column_name]=translation['translatedText']
			insert_into_lookup(column_name,str(df.at[index,hex_column]),str(df.at[index,column_name]))
		except Exception as e:
			log.error(e)

	return df

#---Check if some data(hex code) are not converted--
def preprocess_data_api(df_normal):
	for column_name in HEX_CODE_COLUMN:
		df_hex,df_normal=segregate_rows_in_japanese(df_normal,column_name)

		if len(df_hex) > 0:
			df_hex.to_csv("check_incorrect.csv",index=False)
			df_hex=translate_api_call(df_hex,column_name)
		df_normal=df_normal.append(df_hex,ignore_index=True)
	return df_normal


#---Inserting all japanese code into "cawm_exception" db---
def insert_into_exception_db(connection,df,exception_table):
	with connection.cursor() as cursor:
		for index,row in df.iterrows():
			try:
				if df.at[index,"STOCK_ID"]!=None:			
					sql = "INSERT INTO "+exception_table+" VALUES ("+"\""+str(df.at[index,"UID"])+"\","+"\""+str(df.at[index,"ID"])+"\" ,"+"\""+str(df.at[index,"LOT"])+"\" ,"+"\""+str(df.at[index,"AUCTION_DATE"])+"\" ,"+"\""+str(df.at[index,"AUCTION"])+"\" ,"+"\""+str(df.at[index,"MARKA_ID"])+"\" ,"+"\""+str(df.at[index,"MODEL_ID"])+"\" ,"+"\""+str(df.at[index,"MARKA_NAME"])+"\" ,"+"\""+str(df.at[index,"MODEL_NAME"])+"\" ,"+"\""+str(df.at[index,"YEAR"])+"\" ,"+"\""+str(df.at[index,"ENG_V"])+"\" ,"+"\""+str(df.at[index,"PW"])+"\" ,"+"\""+str(df.at[index,"KUZOV"])+"\" ,"+"\""+str(df.at[index,"GRADE"])+"\" ,"+"\""+str(df.at[index,"COLOR"])+"\" ,"+"\""+str(df.at[index,"KPP"])+"\" ,"+"\""+str(df.at[index,"KPP_TYPE"])+"\" ,"+"\""+str(df.at[index,"PRIV"])+"\" ,"+"\""+str(df.at[index,"MILEAGE"])+"\" ,"+"\""+str(df.at[index,"EQUIP"])+"\" ,"+"\""+str(df.at[index,"RATE"])+"\" ,"+"\""+str(df.at[index,"START"])+"\" ,"+"\""+str(df.at[index,"FINISH"])+"\" ,"+"\""+str(df.at[index,"STATUS"])+"\" ,"+"\""+str(df.at[index,"TIME"])+"\" ,"+"\""+str(df.at[index,"AVG_PRICE"])+"\" ,"+"\""+str(df.at[index,"AVG_STRING"])+"\" ,"+"\""+str(df.at[index,"IMAGES"])+"\" ,"+"\""+str(df.at[index,"LOCAL_TIME"])+"\" ,"+"\""+str(df.at[index,"STOCK_ID"])+"\""+ ")"
					cursor.execute(sql)
					connection.commit()
					log.info("Updating ")
					connection.commit()
					
				else:
					sql = "INSERT INTO "+exception_table+" VALUES ("+"\""+str(df.at[index,"UID"])+"\","+"\""+str(df.at[index,"ID"])+"\" ,"+"\""+str(df.at[index,"LOT"])+"\" ,"+"\""+str(df.at[index,"AUCTION_DATE"])+"\" ,"+"\""+str(df.at[index,"AUCTION"])+"\" ,"+"\""+str(df.at[index,"MARKA_ID"])+"\" ,"+"\""+str(df.at[index,"MODEL_ID"])+"\" ,"+"\""+str(df.at[index,"MARKA_NAME"])+"\" ,"+"\""+str(df.at[index,"MODEL_NAME"])+"\" ,"+"\""+str(df.at[index,"YEAR"])+"\" ,"+"\""+str(df.at[index,"ENG_V"])+"\" ,"+"\""+str(df.at[index,"PW"])+"\" ,"+"\""+str(df.at[index,"KUZOV"])+"\" ,"+"\""+str(df.at[index,"GRADE"])+"\" ,"+"\""+str(df.at[index,"COLOR"])+"\" ,"+"\""+str(df.at[index,"KPP"])+"\" ,"+"\""+str(df.at[index,"KPP_TYPE"])+"\" ,"+"\""+str(df.at[index,"PRIV"])+"\" ,"+"\""+str(df.at[index,"MILEAGE"])+"\" ,"+"\""+str(df.at[index,"EQUIP"])+"\" ,"+"\""+str(df.at[index,"RATE"])+"\" ,"+"\""+str(df.at[index,"START"])+"\" ,"+"\""+str(df.at[index,"FINISH"])+"\" ,"+"\""+str(df.at[index,"STATUS"])+"\" ,"+"\""+str(df.at[index,"TIME"])+"\" ,"+"\""+str(df.at[index,"AVG_PRICE"])+"\" ,"+"\""+str(df.at[index,"AVG_STRING"])+"\" ,"+"\""+str(df.at[index,"IMAGES"])+"\" ,"+"\""+str(df.at[index,"LOCAL_TIME"])+"\" ,"+"NULL"+ ")"
					cursor.execute(sql)
					connection.commit()
					log.info("Updating ")
					connection.commit()
			except Exception as e:
				log.error(e)
				
							
		

#---Check for "nan" value and clean if present---
def clean_nan(df):
	df=df.where(pd.notnull(df),None)
	return df

#---check if any hexcode is present---
def check_exception(df_normal):
	for column_name in HEX_CODE_COLUMN:
		df_hex,df_normal=segregate_rows_in_japanese(df_normal,column_name)
		
		exception_table="history_"+column_name.lower()
		exception_db_connection=connect_to_db(EXCEPTION_DATABASE)
		df_clean=clean_nan(df_hex)
		insert_into_exception_db(exception_db_connection,df_hex,exception_table)
	
	df_normal.to_csv("error.csv",index=False)
	return df_normal


#---Convert all column data into lowercase---
def convert_to_lowercase(df):
	#print(list(df.columns))
	df['COLOR']=df['COLOR'].astype(str).str.lower().str.strip()
	df['RATE']=df['RATE'].astype(str).str.lower().str.strip()
	df['KPP']=df['KPP'].astype(str).str.lower().str.strip()
	df['GRADE']=df['GRADE'].astype(str).str.lower().str.strip()
	return df

#---This function set the clean flag in "history" table---
def set_clean_flag():
	pass

#---Insert into "tmp" table---
def insert_into_db(connection,df):

	with connection.cursor() as cursor:
		for index,row in df.iterrows():
			try:			
				sql = "INSERT INTO "+TMP_TABLE+" VALUES ("+"\""+str(df.at[index,"UID"])+"\","+"\""+str(df.at[index,"ID"])+"\" ,"+"\""+str(df.at[index,"LOT"])+"\" ,"+"\""+str(df.at[index,"AUCTION_DATE"])+"\" ,"+"\""+str(df.at[index,"AUCTION"])+"\" ,"+"\""+str(df.at[index,"MARKA_ID"])+"\" ,"+"\""+str(df.at[index,"MODEL_ID"])+"\" ,"+"\""+str(df.at[index,"MARKA_NAME"])+"\" ,"+"\""+str(df.at[index,"MODEL_NAME"])+"\" ,"+"\""+str(df.at[index,"YEAR"])+"\" ,"+"\""+str(df.at[index,"ENG_V"])+"\" ,"+"\""+str(df.at[index,"PW"])+"\" ,"+"\""+str(df.at[index,"KUZOV"])+"\" ,"+"\""+str(df.at[index,"GRADE"])+"\" ,"+"\""+str(df.at[index,"COLOR"])+"\" ,"+"\""+str(df.at[index,"KPP"])+"\" ,"+"\""+str(df.at[index,"KPP_TYPE"])+"\" ,"+"\""+str(df.at[index,"PRIV"])+"\" ,"+"\""+str(df.at[index,"MILEAGE"])+"\" ,"+"\""+str(df.at[index,"EQUIP"])+"\" ,"+"\""+str(df.at[index,"RATE"])+"\" ,"+"\""+str(df.at[index,"START"])+"\" ,"+"\""+str(df.at[index,"FINISH"])+"\" ,"+"\""+str(df.at[index,"STATUS"])+"\" ,"+"\""+str(df.at[index,"TIME"])+"\" ,"+"\""+str(df.at[index,"AVG_PRICE"])+"\" ,"+"\""+str(df.at[index,"AVG_STRING"])+"\" ,"+"\""+str(df.at[index,"IMAGES"])+"\" ,"+"\""+str(df.at[index,"LOCAL_TIME"])+"\" ,"+"\""+str(df.at[index,"STOCK_ID"])+"\"" + ")"
				cursor.execute(sql)
				connection.commit()
				log.info("Updating ")
				connection.commit()
			except Exception as e:
				if connection:
					log.error(e)	
				else:
					connection=connect_to_db()
					cursor=connection.cursor()
					sql = "INSERT INTO "+TMP_TABLE+" VALUES ("+"\""+str(df.at[index,"UID"])+"\","+"\""+str(df.at[index,"ID"])+"\" ,"+"\""+str(df.at[index,"LOT"])+"\" ,"+"\""+str(df.at[index,"AUCTION_DATE"])+"\" ,"+"\""+str(df.at[index,"AUCTION"])+"\" ,"+"\""+str(df.at[index,"MARKA_ID"])+"\" ,"+"\""+str(df.at[index,"MODEL_ID"])+"\" ,"+"\""+str(df.at[index,"MARKA_NAME"])+"\" ,"+"\""+str(df.at[index,"MODEL_NAME"])+"\" ,"+"\""+str(df.at[index,"YEAR"])+"\" ,"+"\""+str(df.at[index,"ENG_V"])+"\" ,"+"\""+str(df.at[index,"PW"])+"\" ,"+"\""+str(df.at[index,"KUZOV"])+"\" ,"+"\""+str(df.at[index,"GRADE"])+"\" ,"+"\""+str(df.at[index,"COLOR"])+"\" ,"+"\""+str(df.at[index,"KPP"])+"\" ,"+"\""+str(df.at[index,"KPP_TYPE"])+"\" ,"+"\""+str(df.at[index,"PRIV"])+"\" ,"+"\""+str(df.at[index,"MILEAGE"])+"\" ,"+"\""+str(df.at[index,"EQUIP"])+"\" ,"+"\""+str(df.at[index,"RATE"])+"\" ,"+"\""+str(df.at[index,"START"])+"\" ,"+"\""+str(df.at[index,"FINISH"])+"\" ,"+"\""+str(df.at[index,"STATUS"])+"\" ,"+"\""+str(df.at[index,"TIME"])+"\" ,"+"\""+str(df.at[index,"AVG_PRICE"])+"\" ,"+"\""+str(df.at[index,"AVG_STRING"])+"\" ,"+"\""+str(df.at[index,"IMAGES"])+"\" ,"+"\""+str(df.at[index,"LOCAL_TIME"])+"\" ,"+"\""+str(df.at[index,"STOCK_ID"])+"\""+ ")"
					cursor.execute(sql)
					connection.commit()
					log.info("Updating ")
					connection.commit()
				
	return "insertion completed"




#---main function---
def main():
	connection_source=connect_to_db(SOURCE_DATABASE)
	df_history_data=read_data_from_db(connection_source,SQL_NEW_DATA_FETCHING+SOURCE_TABLE+SQL_WHERE_CLEAN_FLAG+" limit 3000")
	df_hex_clean_lookup=preprocess_history_data(df_history_data)
	df_hex_partial_clean=remove_null_values(df_hex_clean_lookup)

	df_clean_api=preprocess_data_api(df_hex_partial_clean)
	df_clean=check_exception(df_clean_api)
	df_lower=convert_to_lowercase(df_clean)
	df_lower.to_csv("lower.csv",index=False)
	status=insert_into_db(connection_source,df_lower)
	log.info(status)


	

#---main function called---
if __name__=="__main__":
	try:
		main()
	except Exception as e:
		print(e)
