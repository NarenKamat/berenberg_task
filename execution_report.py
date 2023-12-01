import pandas as pd
import numpy as np
from utils import config
from utils import Tools

def execution_report():
	#Declare the variables to get the configuration such a input and output file locations

	exe_file = config.get_config('source_files','executionFile') 
	ref_file = config.get_config('source_files','reffile') 
	market_file = config.get_config('source_files','marketfile')
	output_file = config.get_config('output','final_file')

	# Load the source data into panda fdata frames

	df_ex = pd.read_parquet(exe_file, engine ='pyarrow')
	df_ref = pd.read_parquet(ref_file, engine ='pyarrow')
	df_market = pd.read_parquet(market_file, engine ='pyarrow')
 
	# Filter only continuous trading executions

	fil_df_ex = df_ex[df_ex['Phase'] == 'CONTINUOUS_TRADING']

	#Get the count of executions , venues and dates

	count = len(df_ex)
	venues = df_ex.Venue.nunique()
	dates =  df_ex.TradeTime.nunique()
	count_continuous = len(fil_df_ex)


	# Add a column to identify if its a Buy or Sell

	fil_df_ex['side'] = np.where(fil_df_ex['Quantity'] < 0 , 2 , 1)

	# Join the execution and reference dataframes to fetch primary ticker and primary mic and listing id
	df_ex_ref = pd.merge(fil_df_ex, df_ref[["ISIN","Currency","primary_ticker","primary_mic","id"]], on=["ISIN","Currency"], how="inner")
	
	df_ex_ref.rename(columns={"id":"listing_id"},inplace=True)

	# Create a new dataframe from market data dataframe to build unique records with execution_time trimmed at milleseonds format as per the execution dataset
	df_bbo_uniq = df_market[["listing_id","event_timestamp","best_bid_price","best_ask_price"]]
	df_bbo_uniq['TradeTime'] = df_bbo_uniq['event_timestamp'].map(lambda x:  str(x)[:-6])
	df_exb_nodup = df_bbo_uniq.sort_values(by=['listing_id','event_timestamp']).drop_duplicates(subset=['listing_id','TradeTime'], keep='last')

	# Join the unique market dataframe with execution dataframe to fetch the bbo best_price and best_ask
	df_ex_bid_price = pd.merge(df_ex_ref, df_exb_nodup[["listing_id","TradeTime","best_bid_price","best_ask_price"]], on=["listing_id","TradeTime"], how="left")
	df_ex_bid_price.rename(columns={"best_bid_price":"best_bid","best_ask_price":"best_ask"},inplace=True)
	df_ex_bid_price['TradeTime_Sec'] = df_ex_bid_price.TradeTime.astype('datetime64[s]')

	# Build a unique dataframe from original market data dataframe such that the event_timestamp is stored at a second
	df_bbo_1s = df_market[["listing_id","event_timestamp","best_bid_price","best_ask_price"]]
	df_bbo_1s['TradeTime_Sec'] = df_bbo_1s.event_timestamp.astype('datetime64[s]')
	df_bbo_1s_uniq = df_bbo_1s.sort_values(by=['listing_id','event_timestamp']).drop_duplicates(subset=['listing_id','TradeTime_Sec'], keep='last')

	# Add the new derived columns i.e. 1 sec before and after event for price and ask using window functions 
	df_bbo_1s_uniq['best_bid_min_1s'] = df_bbo_1s_uniq.sort_values(by=["listing_id","event_timestamp"], ascending=True).groupby(['listing_id'])['best_bid_price'].shift(1)
	df_bbo_1s_uniq['best_ask_min_1s'] = df_bbo_1s_uniq.sort_values(by=["listing_id","event_timestamp"], ascending=True).groupby(['listing_id'])['best_ask_price'].shift(1)
	df_bbo_1s_uniq['best_bid_1s'] = df_bbo_1s_uniq.sort_values(by=["listing_id","event_timestamp"], ascending=True).groupby(['listing_id'])['best_bid_price'].shift(-1)
	df_bbo_1s_uniq['best_ask_1s'] = df_bbo_1s_uniq.sort_values(by=["listing_id","event_timestamp"], ascending=True).groupby(['listing_id'])['best_ask_price'].shift(-1)
	df_bbo_1s_uniq['mid_price'], df_bbo_1s_uniq['mid_price_min_1s'], df_bbo_1s_uniq['mid_price_1s'] = [(df_bbo_1s_uniq['best_bid_price'] + df_bbo_1s_uniq['best_ask_price'])/2,(df_bbo_1s_uniq['best_bid_min_1s'] + df_bbo_1s_uniq['best_ask_min_1s'])/2, (df_bbo_1s_uniq['best_bid_1s'] + df_bbo_1s_uniq['best_ask_1s'])/2]

	# Create a final dataframe by joining the execution and unique market data dataframe with 1sec metrics 
	df_ex_final = pd.merge(df_ex_bid_price,df_bbo_1s_uniq[['best_bid_min_1s','best_ask_min_1s','best_bid_1s','best_ask_1s','mid_price','mid_price_min_1s','mid_price_1s','listing_id','TradeTime_Sec']], on=["listing_id","TradeTime_Sec"], how="left")

	#Create the slippage metric
	df_ex_final['slippage'] = np.where(df_ex_final['side'] == 2,(df_ex_final['Price'] - df_ex_final['best_bid'])/(df_ex_final['best_ask'] - df_ex_final['best_bid']),(df_ex_final['best_ask'] - df_ex_final['Price'])/(df_ex_final['best_ask'] - df_ex_final['best_bid']))
	df_ex_final.drop(columns=['TradeTime_Sec'], inplace=True)

	df_ex_final.rename_axis('Row',inplace=True)
	# Write the outputs

	t_count = Tools(output_file,'w')
	count_str = f'#_Executions:{count},#_unique_venues:{venues},#_unique_exeDate:{dates},#_Continuos_Trade_Executions:{count_continuous}\n'
	t_count.string_write(count_str)

	t = Tools(output_file,'a')
	t.dataframe_write(df_ex_final,True,True)

if __name__ == "__main__":
	execution_report()
	print("Completed execution report")
