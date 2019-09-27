* prepare the S&P500 firms' aggregate balance sheet, 
* income statement and cash flow statement 
* raw data is from COMPUSTAT -- Fundamental Annual
* the enter and exit year for each company is from 
* COMPUSTAT -- Index Constituents
* adjust for inflation using the CPI2009

cd "/Users/snehakumari/Box Sync/Sneha Summer 2018/sp500/SP500AggFundamental/SP500Agg/From WRDS/2019July"

*----------------------------------------------------------------------------*
///                            CLEAN THE DATA                             ///
*----------------------------------------------------------------------------*
* 1. Filters are the same for balance sheet, income statement, and cash flow
use CashFlow2019n,clear
gen year = year(datadate)
* 1. first clean the COMPUSTAT Data make sure gvkey and year unique identify the firm
*keep if stko  == 0 | stko == 3 	// keep only publicly traded companies, stko=1,2 are subsidiraies, 4 experience LBO									
*drop if naics == ""  			// drop mostly ETF fund									
*keep if fic   == "USA"   		// keep only US incorporated firms, drop 20% of the data (ADR, Canadian firms etc).									
keep if curcd == "USD" 			// only drop 0.2% of the data here, in case financial data is in Canadian dollar but stock price is in US dollar.
									
// confirm that whenever a firm has a FS type of report, it also has a INDL type of report
drop if indfmt == "FS"  		// as a result, I drop all the FS type of report now  

// isid gvkey year 			// gvkey-year is still not the unique identifier
// another reason is some firm might experienced changes in the fiscal year starting/ending month, so 
// one firm can have two reports in the same year. I addressed this by keeping the latest report.
	
bys gvkey year: egen lastrpt = max(datadate)
keep if datadate == lastrpt
isid gvkey year				

save CashFlow2019n_cleaned, replace

*----------------------------------------------------------------------------*
///                 MATCH WITH THE SP500 CONSTITUENTS                     ///
*----------------------------------------------------------------------------*
use SP500Constituents, clear      
keep if conm == "S&P 500 Comp-Ltd"

* merge 
joinby gvkey using CashFlow2019n_cleaned.dta, unmatched(master)  
keep if _merge == 3
drop _merge
gen fromyear = yofd(from)
gen thruyear = yofd(thru)
keep if year >= fromyear & year <= thruyear - 1  

*-------------------------- filter -----------------------
sort gvkey datadate

by gvkey datadate: egen N_FS = total(indfmt == "FS")
by gvkey datadate: egen N_IND = total(indfmt == "INDL")
by gvkey datadate: gen rep =_n

tab N_IND
drop if rep == 2 & N_IND == 2  
tab N_FS 
assert N_IND >= 1 if N_FS == 1 

isid gvkey datadate
egen firmid = group(gvkey)
sort firmid year

* collapse to get the aggregate data 
* balance sheet
collapse (rawsum) aco act ao ap at ceq che dlc dltt intan invt ivaeq ivao lco lct lo lt mib ppent pstk rect seq txditc txp (count)Nfirms = firmid, by(year)
order che rect invt aco act ppent ivaeq ivao intan ao at dlc ap txp lco lct dltt lo txditc mib lt pstk ceq seq

* income statement
collapse (rawsum) cogs cstke dp dvp ib ibadj ibcom mii ni niadj nopi oiadp oibdp pi re sale spi txt xido xint xrd xsga (count)Nfirms = firmid, by(year)
keep sale cogs xsga oibdp dp oiadp xint nopi spi pi txt mii ib dvp ibcom cstke ibadj xido niadj ni xrd Nfirms year
order sale cogs xsga oibdp dp oiadp xint nopi spi pi txt mii ib dvp ibcom cstke ibadj xido niadj ni xrd Nfirms year

* cash flow
collapse (rawsum) aoloch apalch aqc capx chech dlcch dltis dltr dpc dv esubc exre fiao fincf fopo ibc intpn invch ivaco ivch ivncf ivstch oancf prstkc recch siv sppe sppiv sstk txach txdc txpd xidoc (count)Nfirms = firmid, by(year)
order ibc dpc xidoc txdc esubc sppiv fopo recch invch apalch txach aoloch oancf ivch siv ivstch capx sppe aqc ivaco ivncf sstk prstkc dv dltis dltr dlcch fiao fincf exre chech txpd intpn 



// Save nominal file:
export excel using SP500Agg_CashFlow2019_nominaln.xlsx,  replace 

* add the cpi defator to convert the current dollar into real dollar use 2009  = 100
joinby year using cpi2009.dta, unmatched(both) _merge(_merge)
drop if _merge == 2
drop _merge

* convert the current dollar to real dollar
replace cpiaucsl_nbd20090101 = cpiaucsl_nbd20090101/100

* balance sheet
local vlist "che rect invt aco act ppent ivaeq ivao intan ao at dlc ap txp lco lct dltt lo txditc mib lt pstk ceq seq"

* income statement
local vlist " sale cogs xsga oibdp dp oiadp xint nopi spi pi txt mii ib dvp ibcom cstke ibadj xido niadj ni re xrd" 

* cash flow
local vlist "ibc dpc xidoc txdc esubc sppiv fopo recch invch apalch txach aoloch oancf ivch siv ivstch capx sppe aqc ivaco ivncf sstk prstkc dv dltis dltr dlcch fiao fincf exre chech txpd intpn"

foreach v of local vlist{
	replace `v' = `v' / cpiaucsl_nbd20090101  
}


drop if year == 2019


xpose, varname clear
export excel using SP500Agg_CashFlow2019_realn.xlsx,  replace 

















