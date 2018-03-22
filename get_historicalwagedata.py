import requests
import pandas
import zipfile
import os

#------------------------------------------------------------------------------
#The download_file(url) function downloads a big file in chunks.
#courtesy of Roman Podlinov and @danodonovan https://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
def download_file(url):
    local_filename = datapath+url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename
#------------------------------------------------------------------------------

#set path to store files (temporary downloads and final csv file)
datapath="C:/Users/Len/Dropbox/Research/__Data/QCEW/"

#SIC based files are available from 1975 to 2000
#NAICS based files are available from 1990 to 2016 (at time of writing)
#https://www.bls.gov/cew/datatoc.htm

#set naics=False to get a batch of SIC based files, True to get NAICS based
naics=False
startYear=1975
endYear=2000

try: 
    if naics:
        os.remove(datapath+'qcew_wages_industrybyyear_naics.csv')
    else:
        os.remove(datapath+'qcew_wages_industrybyyear_sic.csv')
except OSError:
    pass

for year in range(startYear, endYear+1):
    print('Downloading: https://data.bls.gov/cew/data/files/'+str(year)+'/csv/'+str(year)+'_annual_singlefile.zip')
    
    if naics:
        tempzipfile = download_file('https://data.bls.gov/cew/data/files/'+str(year)+'/csv/'+str(year)+'_annual_singlefile.zip')
    else:
        tempzipfile = download_file('https://data.bls.gov/cew/data/files/'+str(year)+'/sic/csv/sic_'+str(year)+'_annual_singlefile.zip')    
    
    zip_ref = zipfile.ZipFile(tempzipfile, 'r')
    zip_ref.extractall(datapath)
    zip_ref.close()
    os.remove(tempzipfile)
    
    filename=tempzipfile.split('/')[-1]
    filename=filename.split('.')[0]+".csv"
    filename = filename.replace('_', '.')
    
    #dftemp = pandas.read_csv(datapath+filename, nrows=10)
    
    print("Processing year "+str(year))
  
    if naics:
        #variable descriptions: https://data.bls.gov/cew/doc/access/csv_data_slices.htm
        #Columns to keep: 2 industry code, 3 ownership code, 4 aggregation level, 9 annual average employment,10 total annual wages
        #Don't need anymore : 0 area_fips, 5 year, 6 quarter
        #sum((df.qtr!="A")|(df.area_fips!="US000"))
        
        #load the variables we want
        df = pandas.read_csv(datapath+filename, usecols=[0,1,2,3,5,6,9,10])

        #keep only nationally aggregated data for the private sector, by 4-digit NAICS
        #https://data.bls.gov/cew/doc/titles/agglevel/agglevel_titles.htm
        #df.agglvl_code = 16 is "National, by NAICS 4-digit -- by ownership sector"
        #own_code=15 is private (non-government)
        df=df[(df.agglvl_code==16)&(df.own_code==5)]
    else :
        df = pandas.read_csv(datapath+filename)
        #df.agglvl_code= 6 is "National, 4-digit SIC -- by ownership sector"
        #https://data.bls.gov/cew/doc/titles/agglevel/sic_agglevel_titles.htm
        df=df[(df.agglvl_code==6)&(df.own_code==5)]
        
    os.remove(datapath+filename) 
    
    #compute average wages
    df['wage']=df.total_annual_wages/df.annual_avg_emplvl
        
    df["year"] = year
    
    if naics:
        df["naics"] = df.industry_code
        df = df[['year', 'naics', 'wage']]
        with open(datapath+'qcew_wages_industrybyyear_naics.csv', 'a') as f:
            df.to_csv(f, header=(year==startYear), index=False)
    else:
        df["sic"] = df['industry_code']
        df = df[['year', 'sic', 'wage']]
        #df.sic = df.sic.str.replace('SIC_','')
        df.sic = df.sic.str[-4:]
        df.sic = pandas.to_numeric(df.sic)
        with open(datapath+'qcew_wages_industrybyyear_sic.csv', 'a') as f:
            df.to_csv(f, header=(year==startYear), index=False)