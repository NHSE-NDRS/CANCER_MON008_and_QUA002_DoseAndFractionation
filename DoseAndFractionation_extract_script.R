## Dose and Fractionation with new demographics and de-duplication
## The SQL here times-out when run for the entire required period so this script splits the date period into years and runs each one
## Once postcodes of patients are included in V6 this should enable us to more easily obtain required fields

## Also creates (and subsequently deletes) a duplicates reference table in users area 

##Check output area before running


# LIBRARIES
################################################################################
library(dplyr)   
library(data.table)
library(utils)
library(DBI)
library(rJava)
library(RJDBC)
library(svDialogs)
library(getPass)
library(tidyverse)
library(NDRSAfunctions)
library(glue)
d_and_f_folder <- Sys.getenv("folder_rtds_dose_and_fractionation")

################################################################################
# SETUP 

#Set CAS connection details
cas_snapshot <- NDRSAfunctions::createConnection(sid = "cas2407" #Update this to the latest snapshot
                                                 , username = Sys.getenv("analyst_username")
                                                 , port = 1525)

start_date<-as.Date('01-JAN-19',format='%d-%B-%y')
end_date<-as.Date('30-APR-24',format='%d-%B-%y')

#SL comment: the monthDiff is showing how many full months are included and yearDiff is showing the number of years (rounded)there is between the earliest date and the last date 
MonthDiff<-interval (start_date,end_date)%/%months(1)
YearsDiff<-round(MonthDiff/12)

#SL comment: MonthsSeq is showing the number of months to deduct to show full or partial years i.e 58 months of data from 01/01/2019 to 30/11/2023, we dont have a full year
# for 2023, we have 11 months so its 58, minus 11 =47, minus 12 months as we have full 2022 =35 and so on
MonthSeq<-seq(from=11,to=MonthDiff,by=12)

#SL comment: shows the earliest date and the last date included in the extract you are running for any given month 
end_date_=list()
start_date_=list()

#SL comment: from line 59 to line 66 this is a loop that pulls the RTDS data by year as a result of the size of the dataset being extracted, this is then combined later
#SL comment: start_date_[[1]]<-start_date is the start date i.e. 01/01/2019 plus one year so 01/01/2020 and so one until you get to the full 5 years of data 
start_date_[[1]]<-start_date
  
for (i in 1:YearsDiff) {
  
  
end_date_[[i]]<-ceiling_date(start_date%m+%months(MonthSeq[i]),'month')-days(1)
start_date_[[i+1]]<-ceiling_date(start_date%m+%months(MonthSeq[i]),'month')

}

end_date_[[5]]<-end_date
  
  
start_date_<-start_date_[1:i] #SL comment: cuts off the missing full year which was generated in line 60 which had a missing value for 01/01/2024 as that data is not available yet
DatesAll<- cbind.data.frame(as.Date(unlist(start_date_), origin = "1970-01-01") , as.Date(unlist(end_date_), origin = "1970-01-01") )#SL comment: combines all start and end dates
Namez<-c('start_date','end_date')
colnames(DatesAll)<-Namez
DatesAll<-DatesAll%>%mutate_if(is.Date,~format(as.Date(.), "%d/%m/%y"))
 
rm(end_date,end_date_,Namez,start_date,start_date_ ) #SL comment: no longer needed as we have DatesAll
  
  ##Check working
#   check<-paste0('select *
#                 from rtds.at_episodes
#                 fetch first 1 row only')
# 
#   dbGetQueryOracle(snapshot, check)
# rm(check)
  
DoseFrac_Data_=list() #SL comment: this doesn't appear to do anything
DoseFrac_Scr_=list() #SL comment: this doesn't appear to do anything
  
## Creates table of duplicates in your area
#SL comment: some patients, not very many have the same radiotherapy episode id, same treatment date, same diagnosis, same trust but are different patients

Dups<-paste0("create table  ",CAS_user,".Dup as

(select
distinct
t1.radiotherapyepisodeid,
--min(t2.apptdate) as minapptdate,
t2.treatmentstartdate,
t2.patientid,
t2.orgcodeprovider,
tr.Trustname
--,sys_context ( 'USERENV','DB_NAME')
from rtds.at_episodes t2
inner join (
select distinct radiotherapyepisodeid
from rtds.at_episodes
having (count(distinct patientid)>1
and count(distinct orgcodeprovider)=1  )
group by radiotherapyepisodeid
) t1
on t1.radiotherapyepisodeid=t2.radiotherapyepisodeid
left join analysisncr.trustsics tr
on tr.CODE=t2.orgcodeprovider)")

dbGetQueryOracle(snapshot,Dups)

##Check if table was made ok
dbGetQueryOracle(snapshot,paste0("select * from ",CAS_user,".Dup"))

##Main query runs for each year of data requested 
  for (i in 1:nrow(DatesAll)){
DoseFrac_Scr_[i] <-paste0(
"select /*+ USE_HASH(A3 B) */ distinct 
A3.ORGCODEPROVIDER,
B.Country,
B.Provider_name, 
B.RT_network_name,
B.Cancer_alliance_OLD,
B.Cancer_alliance_NEW,
A3.calendar_month,
A3.cancertype,

case
WHEN MIN_INTENT = 1 THEN 'PALLIATIVE'
WHEN MIN_INTENT = 2 THEN 'RADICAL'
ELSE 'OTHER/UNKNOWN' END AS INTENT,            -- THIS BIT SAVES RENAMING LATER

case 
when MIN_INTENT = 2 and round(MAX_PR_EPI_DOSE/MAX_PR_EPI_FRACTIONS,2) < 2 then 'less than 2Gy per fraction'
when MIN_INTENT = 2 and round(MAX_PR_EPI_DOSE/MAX_PR_EPI_FRACTIONS,2) between 2 and 2.49 then 'standard fractionation'
when MIN_INTENT = 2 and round(MAX_PR_EPI_DOSE/MAX_PR_EPI_FRACTIONS,2) between 2.5 and 4.99 then 'mild-moderate hypofractionation'
when MIN_INTENT = 2 and round(MAX_PR_EPI_DOSE/MAX_PR_EPI_FRACTIONS,2) >= 5 then 'ultra hypofractionation'
else 'other/non-curative fractionation' end as fractionation_category,

--A2.max_pr_epi_dose, --QA only edit out for extract
--A2.max_pr_epi_fractions,--QA only edit out for extract
a3.AGE,
a3.ethnicity,
a3.ethnicityname,
a3.sex,
a3.sexname,
a3.IMD19_QUINTILE_LSOAS,  
a3.ICB_HB,
a3.RADIOTHERAPYEPISODEID,

case
when MIN_INTENT = 1 and MAX_PR_EPI_FRACTIONS = 1 then 'single fraction' --added palliative reduced all to fraction groupings
when MIN_INTENT = 1 and MAX_PR_EPI_FRACTIONS between 2 and 5 then '2 to 5 fractions'
when MIN_INTENT = 1 and MAX_PR_EPI_FRACTIONS between 6 and 10 then '6 to 10 fractions'
when MIN_INTENT = 1 and MAX_PR_EPI_FRACTIONS > 10 then 'over 10 fractions'

when CANCERTYPE = 'ANAL' 
and (MAX_PR_EPI_DOSE between 49 and 55 and MAX_PR_EPI_FRACTIONS = 28 and MIN_INTENT = 2) then '50-54 Gy in 28 fractions'
when CANCERTYPE = 'ANAL' 
and (MAX_PR_EPI_DOSE between 39 and 41 and MAX_PR_EPI_FRACTIONS = 28 and MIN_INTENT = 2) then '40 Gy in 28 fractions'
when CANCERTYPE = 'ANAL'  
and (MAX_PR_EPI_DOSE between 19 and 31 and MAX_PR_EPI_FRACTIONS between 5 and 15 and MIN_INTENT = 2)	then '20-30 Gy in 5-15 fractions' --amended to 02 only because palliative just grouped by fractions

when CANCERTYPE = 'BLADDER'
and (MAX_PR_EPI_DOSE between 59 and 65 and MAX_PR_EPI_FRACTIONS between 30 and 32 and MIN_INTENT = 2)then '60-64 Gy in 30-32 fractions'
when CANCERTYPE = 'BLADDER'
and (MAX_PR_EPI_DOSE between 51.5 and 56 and MAX_PR_EPI_FRACTIONS = 20 and MIN_INTENT = 2)then '52.5-55 Gy in 20 fractions'

when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 12 and 25 and MAX_PR_EPI_FRACTIONS between 1 and 5 and MIN_INTENT = 2) then '13-25 Gy in 1-5 fractions' --added July 2024
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 29 and 41 and MAX_PR_EPI_FRACTIONS between 6 and 15 and MIN_INTENT = 2) then '30-40 Gy in 6-15 fractions' --added July 2024
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 46 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2)then '36-45 Gy in 15-20 fractions' --v04 change add
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 33 and MIN_INTENT = 2)then '45-66 Gy in 25-33 fractions' 

when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 47 and 49 and MAX_PR_EPI_FRACTIONS = 15 and MIN_INTENT = 2)then '48 Gy in 15 fractions'--new July 24
when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 41 and MAX_PR_EPI_FRACTIONS =15 and MIN_INTENT = 2)then '40 Gy in 15 fractions' --v05 change simplified label to 40
when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 25 and 27 and MAX_PR_EPI_FRACTIONS =5 and MIN_INTENT = 2)then '26 Gy in 5 fractions'

when CANCERTYPE = 'CERVIX'
and (MAX_PR_EPI_DOSE  BETWEEN 39 and 66 and MAX_PR_EPI_FRACTIONS between 20 and 28 and MIN_INTENT = 2)then '40-65 Gy in 20-28 fractions'

when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 65 and 71 and MAX_PR_EPI_FRACTIONS between 33 and 35 and MIN_INTENT = 2)then '66-70 Gy in 33-35 fractions'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 59 and 67 and MAX_PR_EPI_FRACTIONS between 28 and 30 and MIN_INTENT = 2)then '60-66 Gy in 28-30 fractions'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 56 and MAX_PR_EPI_FRACTIONS between 16 and 20 and MIN_INTENT = 2)then '50-55 Gy in 16-20 fractions'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 53 and 61 and MAX_PR_EPI_FRACTIONS between 30 and 35 and MIN_INTENT = 2)then '54-60 Gy in 30-35 fractions' --added July 2024

when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 53 and 55 and MAX_PR_EPI_FRACTIONS = 36 and MIN_INTENT = 2)then '54 Gy in 36 fractions' --v04 add CHART...to check if affects PIA assessment now or in future
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 33 and MIN_INTENT = 2)then '45-66 Gy in 25-33 fractions'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 56 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2)then '40-55 Gy in 15-20 fractions'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 29 and 61 and MAX_PR_EPI_FRACTIONS between 1 and 8 and MIN_INTENT = 2)then '30-60 Gy in 1-8 fractions'

when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 46 and MAX_PR_EPI_FRACTIONS between 20 and 25 and MIN_INTENT = 2)then '36-45 Gy in 20-25 fractions'
when CANCERTYPE = 'LYMPHOMA' --v04 change 36Gy bottom range
and (MAX_PR_EPI_DOSE BETWEEN 19 and 35 and MAX_PR_EPI_FRACTIONS between 10 and 15 and MIN_INTENT = 2)then '20-34 Gy in 10-15 fractions' --Edited July 2024
when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 19 and 31 and MAX_PR_EPI_FRACTIONS between 5 and 6 and MIN_INTENT = 2)then '20-30 Gy in 5-6 fractions' --v04 change altered group
when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 40 and MAX_PR_EPI_FRACTIONS between 9 and 13 and MIN_INTENT = 2) then '36-39 Gy in 9-13 fractions' --vo4 change new range added

when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 56 and MAX_PR_EPI_FRACTIONS BETWEEN 15 AND 20 and MIN_INTENT = 2)then '50-55 Gy in 15-20 fractions' --Edited July 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 46 and MAX_PR_EPI_FRACTIONS = 10 and MIN_INTENT = 2)then '45 Gy in 10 fractions'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 41 and MAX_PR_EPI_FRACTIONS = 8 and MIN_INTENT = 2)then '40 Gy in 8 fractions'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 34 and 36 and MAX_PR_EPI_FRACTIONS BETWEEN 4 AND 5 and MIN_INTENT = 2)then '35 Gy in 4-5 fractions' --Edited July 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 31.5 and 33.5 and MAX_PR_EPI_FRACTIONS BETWEEN 4 AND 5 and MIN_INTENT = 2)then '32.5 Gy in 4-5 fractions'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 17 and 21 and MAX_PR_EPI_FRACTIONS = 1 and MIN_INTENT = 2)then '18-20 Gy in 1 fraction' --Added jULY 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 67 and MAX_PR_EPI_FRACTIONS BETWEEN 25-33 and MIN_INTENT = 2)then '50-66 Gy in 25-33 fractions' --Added jULY 2024

when CANCERTYPE = 'OESOPHAGUS' 
and (MAX_PR_EPI_DOSE BETWEEN 40.5 and 46 and MAX_PR_EPI_FRACTIONS between 25 and 28 and MIN_INTENT = 2)then '41.5-45 Gy in 25-28 fractions'
when CANCERTYPE = 'OESOPHAGUS' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 30 and MIN_INTENT = 2)then '50-66 Gy in 25-30 fractions' --Edited Gy range July 2024
when CANCERTYPE = 'OESOPHAGUS'
and (MAX_PR_EPI_DOSE BETWEEN 39 and 56 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2)then '40-55 Gy in 15-20 fractions'

when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 65 and 79 and MAX_PR_EPI_FRACTIONS between 33 and 39 and MIN_INTENT = 2)then '66-78 Gy in 33-39 fractions'
when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 51.5 and 61 and MAX_PR_EPI_FRACTIONS = 20 and MIN_INTENT = 2)then '52.5-60 Gy in 20 fractions'
when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 35.25 and 37.25 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2)then '36.25 Gy in 5 fractions'
when CANCERTYPE = 'PROSTATE' 											
and (MAX_PR_EPI_DOSE BETWEEN 36.5 and 38.5 and MAX_PR_EPI_FRACTIONS = 15 and MIN_INTENT = 2)then '37.5 Gy in 15 fractions'																					
when CANCERTYPE = 'PROSTATE' 											
and (MAX_PR_EPI_DOSE BETWEEN 45 and 51 and MAX_PR_EPI_FRACTIONS BETWEEN 23 AND 25 and MIN_INTENT = 2)then '46-50 Gy in 23-25 fractions'										

when CANCERTYPE = 'RECTAL' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 51.4 and MAX_PR_EPI_FRACTIONS between 25 and 28 and MIN_INTENT = 2)then '45-50.4 Gy in 25-28 fractions'
when CANCERTYPE = 'RECTAL' 
and (MAX_PR_EPI_DOSE BETWEEN 24 and 26 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2)then '25 Gy in 5 fractions'

else 'other'
end as dose_fractionation_schedules, 

case --case statement to pull into 2 main groups the selected RCR recommendations and all other schedules
when MIN_INTENT = '01' then 'outside curative RCR schedules' --v04 change 
when CANCERTYPE = 'ANAL' 
and (MAX_PR_EPI_DOSE between 49 and 55 and MAX_PR_EPI_FRACTIONS = 28 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'ANAL' 
and (MAX_PR_EPI_DOSE between 39 and 41 and MAX_PR_EPI_FRACTIONS = 28 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'ANAL'  
and (MAX_PR_EPI_DOSE between 19 and 31 and MAX_PR_EPI_FRACTIONS between 5 and 15 and MIN_INTENT = 2 ) then 'inside RCR schedules' --added 02 only

when CANCERTYPE = 'BLADDER'
and (MAX_PR_EPI_DOSE between 59 and 65 and MAX_PR_EPI_FRACTIONS between 30 and 32 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'BLADDER'
and (MAX_PR_EPI_DOSE between 51.5 and 56 and MAX_PR_EPI_FRACTIONS = 20 and MIN_INTENT = 2)then 'inside RCR schedules'

when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 46 and MAX_PR_EPI_FRACTIONS = 20 and MIN_INTENT = 2)then 'outside selected schedules' --added July 2024
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 33 and MIN_INTENT = 2)then 'inside RCR schedules' 
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 46 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2)then 'inside RCR schedules' --v04 change 
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 12 and 25 and MAX_PR_EPI_FRACTIONS between 1 and 5 and MIN_INTENT = 2) then 'inside RCR schedules' --added July 2024
when CANCERTYPE = 'BRAIN' 
and (MAX_PR_EPI_DOSE BETWEEN 29 and 41 and MAX_PR_EPI_FRACTIONS between 6 and 15 and MIN_INTENT = 2) then 'inside RCR schedules' --added July 2024

when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 51 and MAX_PR_EPI_FRACTIONS = 25 and MIN_INTENT = 2)then 'outside selected schedules'
when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 41 and MAX_PR_EPI_FRACTIONS =15 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 25 and 27 and MAX_PR_EPI_FRACTIONS =5 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'BREAST' 
and (MAX_PR_EPI_DOSE BETWEEN 47 and 49 and MAX_PR_EPI_FRACTIONS = 15 and MIN_INTENT = 2)then 'inside RCR schedules'--new July 24

when CANCERTYPE = 'CERVIX'
and (MAX_PR_EPI_DOSE  BETWEEN 39 and 66 and MAX_PR_EPI_FRACTIONS between 20 and 28 and MIN_INTENT = 2)then 'inside RCR schedules'

when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 65 and 71 and MAX_PR_EPI_FRACTIONS between 33 and 35 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 59 and 67 and MAX_PR_EPI_FRACTIONS between 28 and 30 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 56 and MAX_PR_EPI_FRACTIONS between 16 and 20 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'HEAD AND NECK' 
and (MAX_PR_EPI_DOSE BETWEEN 53 and 61 and MAX_PR_EPI_FRACTIONS between 30 and 35 and MIN_INTENT = 2)then 'inside RCR schedules' --added July 2024

when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 51 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 59 and 61 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 29 and 35 and MAX_PR_EPI_FRACTIONS = 1 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 51 and MAX_PR_EPI_FRACTIONS BETWEEN 4 AND 5 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 51 and MAX_PR_EPI_FRACTIONS = 16 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 51 and MAX_PR_EPI_FRACTIONS = 25 and MIN_INTENT = 2) then 'outside selected schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 53 and 55 and MAX_PR_EPI_FRACTIONS = 36 and MIN_INTENT = 2)then 'inside RCR schedules' --vo4 change added CHART see comment above
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 33 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 56 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'LUNG' 
and (MAX_PR_EPI_DOSE BETWEEN 29 and 61 and MAX_PR_EPI_FRACTIONS between 1 and 8 and MIN_INTENT = 2)then 'inside RCR schedules'

when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 46 and MAX_PR_EPI_FRACTIONS between 20 and 25 and MIN_INTENT = 2)then 'inside RCR schedules' --v04 change 36Gy bottom range
when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 19 and 35 and MAX_PR_EPI_FRACTIONS between 10 and 15 and MIN_INTENT = 2) then 'inside RCR schedules' --Edited July 2024
when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 19 and 31 and MAX_PR_EPI_FRACTIONS between 5 and 6 and MIN_INTENT = 2)then 'inside RCR schedules' --v04 change altered group
when CANCERTYPE = 'LYMPHOMA' 
and (MAX_PR_EPI_DOSE BETWEEN 35 and 40 and MAX_PR_EPI_FRACTIONS between 9 and 13 and MIN_INTENT = 2) then 'inside RCR schedules' --vo4 change new range added

when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 56 and MAX_PR_EPI_FRACTIONS BETWEEN 15 AND 20 and MIN_INTENT = 2)then 'inside RCR schedules' --Edited July 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 46 and MAX_PR_EPI_FRACTIONS = 10 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 39 and 41 and MAX_PR_EPI_FRACTIONS = 8 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 34 and 36 and MAX_PR_EPI_FRACTIONS BETWEEN 4 AND 5 and MIN_INTENT = 2)then 'inside RCR schedules' --Edited July 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 31.5 and 33.5 and MAX_PR_EPI_FRACTIONS BETWEEN 4 AND 5 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 17 and 21 and MAX_PR_EPI_FRACTIONS = 1 and MIN_INTENT = 2)then 'inside RCR schedules' --Added jULY 2024
when CANCERTYPE = 'SKIN' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 67 and MAX_PR_EPI_FRACTIONS BETWEEN 25-33 and MIN_INTENT = 2)then 'inside RCR schedules' --Added jULY 2024


when CANCERTYPE = 'OESOPHAGUS' 
and (MAX_PR_EPI_DOSE BETWEEN 40.5 and 46 and MAX_PR_EPI_FRACTIONS between 25 and 28 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'OESOPHAGUS' 
and (MAX_PR_EPI_DOSE BETWEEN 49 and 67 and MAX_PR_EPI_FRACTIONS between 25 and 30 and MIN_INTENT = 2) then 'inside RCR schedules' --Edited July 2024
when CANCERTYPE = 'OESOPHAGUS'
and (MAX_PR_EPI_DOSE BETWEEN 39 and 56 and MAX_PR_EPI_FRACTIONS between 15 and 20 and MIN_INTENT = 2) then 'inside RCR schedules'

when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 65 and 79 and MAX_PR_EPI_FRACTIONS between 33 and 39 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 51.5 and 61 and MAX_PR_EPI_FRACTIONS = 20 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'PROSTATE' 
and (MAX_PR_EPI_DOSE BETWEEN 35.25 and 37.25 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2) then 'inside RCR schedules'
when CANCERTYPE = 'PROSTATE' 											
and (MAX_PR_EPI_DOSE BETWEEN 36.5 and 38.5 and MAX_PR_EPI_FRACTIONS = 15 and MIN_INTENT = 2) then 'inside RCR schedules'																				
when CANCERTYPE = 'PROSTATE' 											
and (MAX_PR_EPI_DOSE BETWEEN 45 and 51 and MAX_PR_EPI_FRACTIONS BETWEEN 23 AND 25 and MIN_INTENT = 2) then 'inside RCR schedules' --Edited July 2024

when CANCERTYPE = 'RECTAL' 
and (MAX_PR_EPI_DOSE BETWEEN 44 and 51.4 and MAX_PR_EPI_FRACTIONS between 25 and 28 and MIN_INTENT = 2)then 'inside RCR schedules'
when CANCERTYPE = 'RECTAL' 
and (MAX_PR_EPI_DOSE BETWEEN 24 and 26 and MAX_PR_EPI_FRACTIONS = 5 and MIN_INTENT = 2)then 'inside RCR schedules'
else 'outside selected schedules' end as schedule_group,

count (*) over (partition by a3.radiotherapyepisodeid, A3.cancertype, A3.ORGCODEPROVIDER, A3.calendar_month) as count

FROM 

(select 
  ORGCODEPROVIDER,
  calendar_month,
  cancertype,
  MAX_PR_EPI_DOSE, 
  MAX_PR_EPI_FRACTIONS,
  age,
  ethnicity,
  ethnicityname,
  sex,
  sexname,
  IMD19_QUINTILE_LSOAS,
  ICB_HB,
  RADIOTHERAPYEPISODEID,
  MIN_INTENT,
   row_number()	
  OVER(PARTITION BY  treatmentstartdate, radiotherapyepisodeid, orgcodeprovider, min_intent,  cancertype,
       MAX_PR_EPI_DOSE, 
       MAX_PR_EPI_FRACTIONS,
       age,
       ethnicity,
       ethnicityname,
       sex,
       sexname
       ORDER BY	
       (CASE WHEN dateofaddress IS NULL THEN	
         9999	
         WHEN dateofaddress <= treatmentstartdate THEN	
         TO_DATE('1800-01-01', 'yyyy-mm-dd') - dateofaddress	
         ELSE	
         dateofaddress - treatmentstartdate	
         END	
       ) ASC , addressid asc	
  ) AS rn_address
  
  from
  
  (
    select distinct 
    a1.ORGCODEPROVIDER,
    a1.calendar_month,
    a1.TREATMENTSTARTDATE,
    a1.cancertype,
    MAX_PR_EPI_DOSE, 
    MAX_PR_EPI_FRACTIONS,--don't edit out from inner query
      
        case when Tx_age <40 then '< 40'               --v04 change to age groups avoids manual recalc in SAS
        when Tx_age between 40 and 49 then '40 - 49'
        when Tx_age between 50 and 59 then '50 - 59'
        when Tx_age between 60 and 69 then '60 - 69'
        when Tx_age between 70 and 79 then '70 - 79'
        when Tx_age between 80 and 89 then '80 - 89'
        when Tx_age >= 90 then '90+'
         else 'unknown' end as age,
    ethnicity,
    ethnicityname,
    sex,
    sexname,
IMD19_QUINTILE_LSOAS,
ICB_HB,
    addressid,
    dateofaddress,
     a1.radiotherapyepisodeid,
      MIN (A1.ORDERED_INTENT) Over (Partition BY A1.RADIOTHERAPYEPISODEID, A1.ORGCODEPROVIDER) AS MIN_INTENT
      
      FROM
    
(
 /* core qry start */
select  /*+ USE_HASH(N A) USE_HASH(N HA) USE_HASH(N icb)  USE_HASH(N IMD)*/
    E.PATIENTID, 
    DECODE(E.ORGCODEPROVIDER,'RH1','R1H','RP6','R1H','RNL','RNN','RGQ','RDE','RQ8','RAJ','RDD','RAJ','RBA','RH5','RA3','RA7','RDZ','R0D','RD3','R0D','RNJ','R1H','RXH','RYR','E0A','RYR',E.ORGCODEPROVIDER) AS ORGCODEPROVIDER, 
        TO_CHAR(E.TREATMENTSTARTDATE, 'YYYY/MM') AS CALENDAR_MONTH,
        
    --p.rttreatmentanatomicalsite, --...edit out just for checking 03 intents
    --p.RTTREATMENTREGION, --...edit out just for checking 03 intents
    E.RADIOTHERAPYDIAGNOSISICD,
    E.RADIOTHERAPYINTENT,
    E.RADIOTHERAPYEPISODEID,
    E.TREATMENTSTARTDATE,
    P.PRESCRIPTIONID,
    p.rtprescribeddose,
    p.prescribedfractions,
    p.rttreatmentmodality,
    trunc((e.treatmentstartdate - pa.birthdatebest) /365.25) as Tx_age, 
   first_value(pa.ethnicity) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ethnicity,
   first_value(pa.ethnicityname) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ethnicityname,
    first_value(pa.sex) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as sex,
    case when first_value(pa.sex) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) in (0,NULL, 9) then 'Not Known/ Not Specified/Null'
    when first_value(pa.sex) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) =1 then 'Male'
    when first_value(pa.sex) IGNORE NULLS over(partition by e.orgcodeprovider, e.radiotherapyepisodeid order by E.TREATMENTSTARTDATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)=2 then 'Female'
   else 'Unknown' end as SexName, --Based on conversion table springmvc3.ZSEX
   A.addressid,
   A.dateofaddress,   
        CASE       WHEN substr(N.lsoa11, 1, 1) != 'E' or N.lsoa11 is null 
					THEN  HA.HLTHAUNM 
                    ELSE icb.icb22NM END AS ICB_HB,   
        IMD.IMD19_QUINTILE_LSOAS,

   
     CASE 
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11','C12','C13','C14','C30','C31','C32') then 'HEAD AND NECK' 
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C50','D05') then 'BREAST' --added in DCIS D05
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C15') then 'OESOPHAGUS'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C81','C82','C83','C84','C85','C86') then 'LYMPHOMA'--v2 change KS added C83-85 back in previous months have been C81,C82 only. SL added C86 on 18/01/2024.
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C61') then 'PROSTATE'
     --Lung updated July 2024 following request from Ctaherin Roe
     --For all lung tumour groupings can you restrict the treatment region to P,R, PR, A and M exclude O and NULL.
     when (substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C33','C34') and RTTREATMENTREGION !='O') then 'LUNG'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C20') then 'RECTAL'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C53') then 'CERVIX'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C21') then 'ANAL'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C67') then 'BLADDER'
     when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C70','C71','D32','D33') then 'BRAIN' --excludes PITUITARY 
	 when substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C44') then 'SKIN'
     else 'OTHER ICD CODES' end AS CANCERTYPE,
  
    MAX (P.RTPRESCRIBEDDOSE) Over (Partition BY E.RADIOTHERAPYEPISODEID, E.ORGCODEPROVIDER) AS MAX_PR_EPI_DOSE,
    MAX (P.PRESCRIBEDFRACTIONS) Over (Partition BY E.RADIOTHERAPYEPISODEID, E.ORGCODEPROVIDER ) AS MAX_PR_EPI_FRACTIONS,
    CASE 
    WHEN E.RADIOTHERAPYINTENT IN ('01') THEN 1
       WHEN E.RADIOTHERAPYINTENT IN ('02') THEN 2
       WHEN E.RADIOTHERAPYINTENT IN ('03') THEN 3 
       ELSE 4 END AS ORDERED_INTENT              -- IN CASE OF BLANKS
    
     
    from RTDS.AT_EPISODES E --include Welsh patients
    
    INNER JOIN  RTDS.AT_PRESCRIPTIONS P
     ON  E.ATTENDID              = P.ATTENDID
     AND E.ORGCODEPROVIDER       = P.ORGCODEPROVIDER
     AND E.RADIOTHERAPYEPISODEID = P.RADIOTHERAPYEPISODEID
     AND E.PATIENTID             = P.PATIENTID 
     INNER JOIN RTDS.AT_EXPOSURES X --table added to add back in cyber/gammaknife machines?		
   ON P.ORGCODEPROVIDER        = X.ORGCODEPROVIDER			
   AND P.ATTENDID              = X.ATTENDID			
   AND P.RADIOTHERAPYEPISODEID = X.RADIOTHERAPYEPISODEID			
   AND P.PATIENTID             = X.PATIENTID
   AND P.PRESCRIPTIONID        = X.PRESCRIPTIONID
   LEFT JOIN RTDS.at_patient PA
   ON E.PATIENTID  = PA.PATIENTID
   
       LEFT JOIN springmvc3.address                             A ON E.patientid = A.patientid	
      LEFT JOIN nspl_202305@casref01 N ON A.postcode= N.pcd	
      left join analysisncr.hlthau_nc_uk_201904@casref01 HA on HA.HLTHAUCD=n.HLTHAU
       LEFT JOIN analysisncr.lsoa_loc_icb_la_202207@casref01    icb ON N.lsoa11 = icb.lsoa11cd 
      left join IMD.IMD2019_EQUAL_LSOAS@casref01 IMD on IMD.LSOA11_CODE=N.LSOA11
      
      ---This is my own table. Need to think of a long term way of storing this. Ask analysts when it comes closer to publishing. 
      left join ",CAS_user,".DUP  DUP
       on  DUP.radiotherapyepisodeid=E.radiotherapyepisodeid 
      and DUP.treatmentstartdate=E.treatmentstartdate
      and DUP.orgcodeprovider=E.orgcodeprovider
      and DUP.patientid=E.patientid  
   
  --'01-JAN-19
  WHERE TO_DATE(e.TREATMENTSTARTDATE) BETWEEN TO_DATE('",DatesAll[i,1],"', 'DD/MM/YY') AND TO_DATE(' ",DatesAll[i,2]," 23:59:00', 'DD/MM/YY HH24:MI:SS') 
  --and e.radiotherapyintent = '03' --check on what the 03 codes constitute...
  --and E.ORGCODEPROVIDER IN ('7A1','7A3','RQF') --includes Welsh trusts sumbitting data 
  and  (p.rttreatmentmodality = '05'
        or x.machineid in ('RA7OT1887','RHQOT1816','RHQOT1817','R1HOT2041','R1HOT3116','RTHOT3117','RR8OT3006','RNJLA1743','RWHLA1685','RRKLA1819','RPYLA1894','RPYLA1978'))--incl cyber/gamma machines under modality 06 (a lot brain mets so are they needed?)
  and substr(E.RADIOTHERAPYDIAGNOSISICD,1,3) IN ('C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11','C12','C13','C14','C30','C31','C32',
                                                 'C50','D05','C15','C33','C34','C44','C20','C53','C21','C67','C81','C82','C83','C84','C85','C86','C61','C70','C71','D32','D33') --RG change added back in C83-C85 lymphoma. SL added C86 on 18/01/2024
  and (DUP.radiotherapyepisodeid is null)
  --;
  --/* core qry end *//*
  ) A1--selected sites DO NOT use query for TOTAL RTDS monthly episode counts
    
    WHERE Tx_age >=18
    
) A2 )A3 

LEFT JOIN ANALYSISCATHERINEOKELLO.RT_NETWORKS_20201001_UK@CASREF01 B --master lookup table from RTDS Guidance folder NB changed Wales and other Provider Names 
ON A3.ORGCODEPROVIDER = B.PROVIDER_CODE
where a3.rn_address=1")

#SL comments: combines each year of data into one file
}
## Gets the data for each year - check each year is populated re-run any if server disconnects
    
for (i in 1:nrow(DatesAll)){
DoseFrac_Data_[[i]]<-dbGetQueryOracle(snapshot,DoseFrac_Scr_[[i]])}

##Binds rows for full dataset
All_DoseFrac<-bind_rows(DoseFrac_Data_)

##Counts the number of times each radiotherapyepisodeid appears (fixs any issues with counts being off because of cross year appearances)
##Changed grouping to remove duplicates
All_DoseFrac<-All_DoseFrac%>%group_by(RADIOTHERAPYEPISODEID,PROVIDER_NAME,CANCERTYPE,CALENDAR_MONTH)%>%mutate(COUNT=n())

## clean up environment
rm(DoseFrac_Data_,DoseFrac_Scr_)

##Remove duplicate table

DupTableDelete<-paste0("drop table ",CAS_user,".DUP")

dbGetQueryOracle(snapshot,DupTableDelete)

##Disconnect from the server
dbDisconnect(snapshot)

##Create file name with max month

MaxDate<-max(All_DoseFrac$CALENDAR_MONTH)
MaxDate<-paste0(substr(MaxDate,6,7),substr(MaxDate,1,4))


## Make txt file - change file location if necessary

fwrite(All_DoseFrac, 
       file = glue::glue("{d_and_f_filepath}{MaxDate}_WO.txt"), 
       sep = "|",
       row.names = FALSE)

##If you want to save an R file
# getwd()
# setwd(~)
# save(All_DoseFrac,file='All_DoseFrac.RData')

View(All_DoseFrac)


