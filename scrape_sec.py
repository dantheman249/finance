#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import packages
from datetime import datetime
import lxml
from lxml import html, etree
import requests
import numpy as np
import pandas as pd
import time
from time import sleep
import string
import yfinance as yf
from bs4 import BeautifulSoup
import urllib
import io
import os


# In[2]:


# let's first make a function that will make the process of building a url easy.
def make_url(base_url , comp):
    
    url = base_url
    
    # add each component to the base url
    for r in comp:
        url = '{}/{}'.format(url, r)
        
    return url

# function to get unique values 
def unique(list1): 
    x = np.array(list1) 
    return(np.unique(x)) 


# In[44]:


#returns a list of 10-k urls 
def pull_10k_urls():

    start = time.time()
    print("Starting to Pull All 10K URLs ")

    list_10k = []

    year_list = ['2015', '2016', '2017', '2018', '2019']
    for y in year_list: 
        print("Pulling Year: "+y)
        #get all 2019 10ks into dataframe of company, date, balance sheet, income statement, cash flow statement

        # PULL DAILY INDEX FILINGS 
        base_url = r"https://www.sec.gov/Archives/edgar/daily-index"

        # The daily-index filings, require a year and content type (html, json, or xml).
        year_url = make_url(base_url, [y, 'index.json'])

        # Display the new Year URL
        # print('-'*100)
        # print('Building the URL for Year: {}'.format('2019'))
        # print("URL Link: " + year_url)

        # request the content for 2019, remember that a JSON strucutre will be sent back so we need to decode it.
        content = requests.get(year_url)
        decoded_content = content.json()

        # def get_10k():
        for item in decoded_content['directory']['item']:

            # get the name of the folder
            print('-'*100)
            print('Pulling url for Quarter: {}'.format(item['name']))

            # The daily-index filings, require a year, a quarter and a content type (html, json, or xml).
            qtr_url = make_url(base_url, [y, item['name'], 'index.json'])

            # print out the url.
            print("URL Link: " + qtr_url)

            # Request, the new url and again it will be a JSON structure.
            file_content = requests.get(qtr_url)
            sleep(0.2)
            decoded_content = file_content.json()

            print('-'*100)
            print('Pulling files')

            #get just the master file urls into a df 
            master_url_list = []
            for file in decoded_content['directory']['item']:
                if "master" in file['name']:
                    file_url = make_url(base_url, [y, item['name'], file['name']])

                    master_url_list.append(file_url)
                    #print("File URL Link: " + file_url)
                    #sleep(2)
                else:
                    pass
            #print(master_url_df) 

            #traverse the master url files 
            for u in master_url_list[0:5]: 
                #get content of the file 
                print("Requesting: " + u)
                content = requests.get(u).content
                sleep(0.2)  

                try: 
                    data = content.decode("utf-8").split('  ')
                    # We need to remove the headers, so look for the end of the header and grab it's index
                    for index, item in enumerate(data):
                        if "ftp://ftp.sec.gov/edgar/" in item:
                            start_ind = index
                    data_format = data[start_ind + 1:]
                    #list to store master data info 
                    master_data = []
                    # now we need to break the data into sections, this way we can move to the final step of getting each row value.
                    for index, item in enumerate(data_format):

                        # if it's the first index, it won't be even so treat it differently
                        if index == 0:
                            clean_item_data = item.replace('\n','|').split('|')
                            clean_item_data = clean_item_data[8:]
                        else:
                            clean_item_data = item.replace('\n','|').split('|')

                        for index, row in enumerate(clean_item_data):

                            # when you find the text file.
                            if '.txt' in row:

                                # grab the values that belong to that row. It's 4 values before and one after.
                                mini_list = clean_item_data[(index - 4): index + 1]

                                if len(mini_list) != 0:
                                    mini_list[4] = "https://www.sec.gov/Archives/" + mini_list[4]
                                    master_data.append(mini_list)
    #                                 print(mini_list)
                            #print(master_data)

                        master_data = pd.DataFrame.from_records(master_data, columns = ['cik', 'name', 'form','date', 'url'])
                       # print(master_data)
                        df_10k = master_data[master_data['form'] == "10-K"]
                        print(len(df_10k))
                        if len(df_10k) > 0:
                            list_10k.append(df_10k)
                            print(df_10k)
                        #print(list_10k)
                except Exception as ex: 
                    print("url failed: "+ u)
                    pass

    #End Code
    end = time.time()
    print(end - start)
    print("Finished Pulling 10K URLs")
    return(list_10k)
    

url_list_10k = pull_10k_urls()


# In[47]:


df_10k = pd.concat(url_list_10k)
#df_10k["cik"] = df_10k["cik"].apply(str)
#write 10k links to csv
df_10k.to_csv("df_10k.csv")

print(df_10k.shape)
print(df_10k)


# In[48]:


#read in 10k df 
df_10k_extract = pd.read_csv("df_10k.csv")
df_10k_extract.shape
#df_10k_extract["cik"] = df_10k_extract["cik"].apply(str)


# In[49]:


print(df_10k_extract)


# In[50]:


# get tickers of major companies 
sp1000=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_1000_companies')
sp1000 = pd.DataFrame(sp1000[5]['Ticker symbol'])
sp1000.columns = ['ticker']
sp1000["ticker"] = sp1000["ticker"].str.lower()
#print(sp1000)

sp500 = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
sp500 = pd.DataFrame(sp500[0]['Symbol'])
sp500.columns = ['ticker']
sp500["ticker"] = sp500["ticker"].str.lower()
#print(sp500)


#61 dividend aristocrats
div_arist = pd.read_html("https://en.wikipedia.org/wiki/S%26P_500_Dividend_Aristocrats")
div_arist = pd.DataFrame(div_arist[2]['Ticker Symbol'])
div_arist.columns = ["ticker"]
div_arist["ticker"] = div_arist["ticker"].str.lower()
#print(div_arist)

# secs = pd.concat([sp500, sp1000], axis=0)
# secs = secs.replace(" ", "")
# secs = secs.reset_index(drop=True)


# In[54]:


#get ticker, company name and CIK numbers 
ticker_url = "https://www.sec.gov/include/ticker.txt"
ticker_request = requests.get(ticker_url).content
ticker_df = pd.read_csv(io.StringIO(ticker_request.decode('utf-8')),sep="\t")
ticker_df.columns = ['ticker', 'cik']
ticker_df["ticker"] = ticker_df["ticker"].str.lower()
#print(ticker_df[0:5])

#reduce ciks to dividend aristocrats 
sample_ticker_df = pd.merge(ticker_df,sp500,on='ticker')
#sample_ticker_df['cik'] = sample_ticker_df['cik'].apply(str)
#print(sample_ticker_df)

#print(df_10k_extract['cik'])

#reduce 10ks to sample_df
sample_10k_df= df_10k_extract[0:30]
#pd.merge(df_10k_extract, sample_ticker_df,  on='cik')
#print(sample_10k_df['url'][0])
print(sample_10k_df)


# In[55]:


#iterate through 10k urls and return df of urls that lead to filing summary 
print("Start Pulling Filing Summaries")
start = time.time()
summary_url_df = []

base_url = r"https://www.sec.gov"

for index, row in sample_10k_df.iterrows():
    #print(row)
    doc_url = row['url'].replace('-','').replace('.txt','/index.json')
    #print("Requesting: "+doc_url)
    content = requests.get(doc_url).json()
    #rate limit of 10/second
    sleep(0.2)
    
    for file in content['directory']['item']:
        # Grab the filing summary and create a new url leading to the file so we can download it.
        if file['name'] == 'FilingSummary.xml':
            xml_summary = base_url + content['directory']['name'] + "/" + file['name']
            new_row = [row['cik'], row['name'], row['date'],  xml_summary]
            summary_url_df.append(new_row)
            
summary_url_df = pd.DataFrame.from_records(summary_url_df, columns = ['cik', 'name', 'date', 'xml_summary'])
#print(summary_url_df['xml_summary'][0])

#write summary urls to a csv
summary_url_df.to_csv("summary_urls.csv")

#End Code
end = time.time()
print(end - start)
print("Finished Pulling Filing Summaries")


# In[64]:


#read in summary urls from csv 
summary_url_extract = pd.read_csv("summary_urls.csv")
print(summary_url_extract.head())
print(summary_url_extract.shape)
print(summary_url_extract['xml_summary'][0])


# In[65]:


#iterate through df and get financial data reports in a master list of dictionaries
print("Starting to pull reports")
start = time.time()

master_reports = []

stmt_list = []

category_list = []
for index, row in summary_url_extract.iterrows():
    xml = row['xml_summary']
    base_url = row['xml_summary'].replace('FilingSummary.xml', '')
    #print(xml)
    content = requests.get(xml).content
    sleep(0.2)
    soup = BeautifulSoup(content, 'lxml')
    # find the 'myreports' tag because this contains all the individual reports submitted.
    reports = soup.find('myreports') 
    #print(reports) 

#     # loop through each report in the 'myreports' tag but avoid the last one as this will cause an error.
    for report in reports.find_all('report')[:-1]:

        # let's create a dictionary to store all the different parts we need.
        report_dict = {}
        report_dict['name_short'] = report.shortname.text.lower()
        report_dict['name_long'] = report.longname.text
        report_dict['position'] = report.position.text
        report_dict['category'] = report.menucategory.text
        report_dict['url'] = base_url + report.htmlfilename.text
        
        # only add statements 
        if report.menucategory.text == "Statements": 

            # append the dictionary to the master list.
            master_reports.append(report_dict)

            stmt_list.append(report_dict['name_short'])
            category_list.append(report_dict['category'])
            #print(report_dict)
        else: 
            pass

#         # print the info to the user.
#         print('-'*100)
#         print(base_url + report.htmlfilename.text)
#         print(report.longname.text)
#         print(report.shortname.text)
#         print(report.menucategory.text)
#         print(report.position.text)


#End Code
end = time.time()
print(end - start)
print("Finished Pulling Reports")


# In[25]:


print(master_reports)


# In[ ]:


# print(unique(category_list))


# In[ ]:


print(unique(stmt_list))


# In[ ]:


# create the list to hold the statement urls
statements_url = []

for report_dict in master_reports:
    
    # define the statements we want to look for.
    item1 = r"consolidated balance sheets"
    item2 = r"consolidated statements of earnings"
    item3 = r"consolidated statements of cash flows"
    item4 = r"consolidated statements of shareholders' equity"
    
    # store them in a list.
    report_list = [item1, item2, item3, item4]
    
    # if the short name can be found in the report list.
    if report_dict['name_short'] in report_list:
        
        # print some info and store it in the statements url.
        print('-'*100)
        print(report_dict['name_short'])
        print(report_dict['url'])
        
        statements_url.append(report_dict['url'])
    else: 
        pass
        #print(report_dict['name_short'])


# In[ ]:


# let's assume we want all the statements in a single data set.
statements_data = []

# loop through each statement url
for statement in statements_url:

    # define a dictionary that will store the different parts of the statement.
    statement_data = {}
    statement_data['headers'] = []
    statement_data['sections'] = []
    statement_data['data'] = []
    
    # request the statement file content
    content = requests.get(statement).content
    sleep(1)
    report_soup = BeautifulSoup(content, 'html')

    # find all the rows, figure out what type of row it is, parse the elements, and store in the statement file list.
    for index, row in enumerate(report_soup.table.find_all('tr')):
        
        # first let's get all the elements.
        cols = row.find_all('td')
        
        # if it's a regular row and not a section or a table header
        if (len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0): 
            reg_row = [ele.text.strip() for ele in cols]
            statement_data['data'].append(reg_row)
            
        # if it's a regular row and a section but not a table header
        elif (len(row.find_all('th')) == 0 and len(row.find_all('strong')) != 0):
            sec_row = cols[0].text.strip()
            statement_data['sections'].append(sec_row)
            
        # finally if it's not any of those it must be a header
        elif (len(row.find_all('th')) != 0):            
            hed_row = [ele.text.strip() for ele in row.find_all('th')]
            statement_data['headers'].append(hed_row)
            
        else:            
            print('We encountered an error.')

    # append it to the master list.
    statements_data.append(statement_data)


# In[ ]:


#print(statements_data)
#print(statements_data[3]['headers'][0])
#print(statements_data[1]['data'])


# In[ ]:


#grab income statement 


# Grab the proper components
income_header =  statements_data[0]['headers'][1]
print(income_header)


income_data = statements_data[0]['data']

# Put the data in a DataFrame
income_df = pd.DataFrame(income_data)

# Display
print('-'*100)
print('Before Reindexing')
print('-'*100)
display(income_df.head())

# Define the Index column, rename it, and we need to make sure to drop the old column once we reindex.
income_df.index = income_df[0]
income_df.index.name = 'Category'
income_df = income_df.drop(0, axis = 1)

# Display
print('-'*100)
print('Before Regex')
print('-'*100)
display(income_df.head())

# Get rid of the '$', '(', ')', and convert the '' to NaNs.
income_df = income_df.replace('[\$,)]','', regex=True )                     .replace( '[(]','-', regex=True)                     .replace( '', 'NaN', regex=True)

# Display
print('-'*100)
print('Before type conversion')
print('-'*100)
display(income_df.head())

# everything is a string, so let's convert all the data to a float.
income_df = income_df.astype(float)

# Change the column headers
income_df.columns = income_header

# Display
print('-'*100)
print('Final Product')
print('-'*100)

# show the df
income_df

# drop the data in a CSV file if need be.
# income_df.to_csv('income_state.csv')


# In[ ]:


#given a list of cik numbers, retrieve financial statements from the last 10 years. 

def retrieve_financials(cik_list):


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




