from datetime import datetime
from concurrent import futures
from concurrent.futures import wait

import pandas as pd
from pandas import DataFrame
from pandas_datareader import data as pdr
import yfinance as yf

import os
import csv




def download_stock(stock):
    #""" try to query the yahoo finance for a stock, if failed note with print """
    #try:

        download_stock_history(stock)
        download_stock_info(stock)

    #except Exception as e:
    #    print(e)
    #    invalid_stocks.append(stock)
    #    print('invalid stocks: %s' % (stock))


def download_stock_history(stock):

    yf.pdr_override()
    start_time_string = start_time.strftime("%Y") + "-" + start_time.strftime("%m") + "-" + start_time.strftime("%d")
    now_time_string = now_time.strftime("%Y") + "-" + now_time.strftime("%m") + "-" + now_time.strftime("%d")
    stock_df = pdr.get_data_yahoo(stock, start=start_time_string, end=now_time_string)
    stock_df['Name'] = stock
    output_path = "stocks/" + stock + '/'
    output_name = output_path + 'history_' + stock +'.csv'

    if not os.path.exists(output_path):
        # Create a new directory because it does not exist
        os.makedirs(output_path)

    stock_df.to_csv(output_name)

def download_stock_info(stock):


    tickers = yf.Tickers(stock)
    current_stock = tickers.tickers[stock]

    output_name = 'stocks/' + stock + '/info_' + stock + '.csv'

    df = pd.DataFrame({'info:' : [current_stock.info]})
    df.to_csv(output_name, header=True)


    with open(output_name,'a+', newline='') as writer:
        writer.write("---CashFlow---\n")

    df = pd.DataFrame(current_stock.cashflow)
    df.to_csv(output_name, mode='a', header=True)


    with open(output_name,'a+', newline='') as writer:
        writer.write("---Financials---\n")

    df_financials = pd.DataFrame(current_stock.financials)
    df_financials.to_csv(output_name, mode='a', header=True,)

    #balancesheet, recommendations, earnings, actions, options, shares
    #

    ##
    # todo remove save
    # todo compute Fundamentals according to Ebook
    # todo do analysis and confirmation on stocks history
    ##




if __name__ == '__main__':
    print(yf.__version__)
    """ time range """
    now_time = datetime.now()
    start_time = datetime(now_time.year - 10, now_time.month , now_time.day)

    """ list of stocks """
    stock_list = ['AMZN','RS2K.PA']
    #stock_list = ['AMZN','RS2K.PA','ETSZ.DE','PE500.PA','PAASI.PA','PTPXE.PA']

    invalid_stocks =[] #failed stocks

    """ parallelize the stock scrapping using the concurrent.futures module's ThreadPoolExecutor """


    #set the maximum thread number
    #max_workers = 50

    #workers_number = min(max_workers, len(stock_list))
    #in case a smaller number of stocks than threads was passed in
    #with futures.ThreadPoolExecutor(workers_number) as executor:
    #    futures = [executor.submit(download_stock, stock_list[i]) for i in range(workers_number)]
    #    while(executor._work_queue.qsize()>0):
    #        pass
    #    wait(futures)
    for i in range(len(stock_list)):
        download_stock(stock_list[i])

    """ Save failed queries to a text file to retry """
    if len(invalid_stocks) > 0:
        with open('failed_queries.txt','w') as outfile:
            for name in invalid_stocks:
                outfile.write(name+'\n')

    #timing:
    finish_time = datetime.now()
    duration = finish_time - now_time
    minutes, seconds = divmod(duration.seconds, 60)

    print(f'The threaded script took {minutes} minutes and {seconds} seconds to run.')