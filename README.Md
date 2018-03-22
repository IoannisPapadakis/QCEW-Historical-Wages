## Download historical industry by year wage averages from the BLS Quarterly Census of Employment and Wages

The [Quarterly Census of Employment and Wages](https://www.bls.gov/cew/) by the BLS contains wage and employment data at the county by industry code by year level since 1975.
However, the data is only easily queryable through their API for the most historical five years. Historical data appears to be avilable only though the files listed [here](https://www.bls.gov/cew/datatoc.htm).
For my purposes, I wanted average wages by industry by year, as far back as possible. Doing this manually would have involved downloading one file for each year, unzipping it, and extracting the relevant aggregate statistics from the CSV. This script automates that process.

The Python script [get_historicalwagedata.py](get_historicalwagedata.py) is set up for this purpose specifically, but could be easily be adapted to suit other purposes (e.g. extract data by geographical area rather than industry). To use as is, simply change the following lines in the preamble, as desired:

```{python}
#set path to store files (temporary downloads and final csv file)
datapath="C:/Users/Len/Dropbox/Research/__Data/QCEW/"

#SIC based files are available from 1975 to 2000
#NAICS based files are available from 1990 to 2016 (at time of writing)
#https://www.bls.gov/cew/datatoc.htm

#set naics=False to get a batch of SIC based files, True to get NAICS based
naics=False
startYear=1975
endYear=2000
```
The QCEW data has been aggregated by NAICS code since 2000, and is availble aggregated by SIC code from 1975 to 2000. The code can operate either by SIC code or NAICS code, by setting the ```naics``` boolean variable. **Note:** as written, the code will extract data aggregated at the four digit NAICS level (this could be changed easily). The results from the script (for the full 1975-2000 SIC-based data, and the full 1990-2016 NAICS-based data). Are available in the repository as [qcew_wages_industrybyyear_naics.csv](qcew_wages_industrybyyear_naics.csv) and [qcew_wages_industrybyyear_sic.csv](qcew_wages_industrybyyear_sic.csv).

Note: After each file is downloaded, it is read into Python and then deleted. The extracted data is appended across years into a file named ```qcew_wages_industrybyyear_naics.csv``` if ```naics``` is set to True and ```qcew_wages_industrybyyear_sic.csv``` if ```naics``` is set to False, in the local directory specified by ```datapath```.

Please let me know if you have any concerns or catch a mistake!
