#	RTDS COVID-19 CancerData Dashboard - Aggregation code  ###############
#
#	takes the underlying extract that feeds the CancerStats2 version of this dashboard 
#	and aggregates it to the specifications of the CancerData dashboard
#
# 05/12/2023 New version LM
# Adds demographics and changes format to same as SACT dashboard
# Dose part added as per Carmen's suggestion
# 22/12/2023 change to add Jack's automatic suppression method for low numbers
# Also to include low numbers for unknown deprivation at CA/ICS level in line with SACT DB
# 12/01/2024 changed to allow unknown/others for ethnicity and intent
# 16/01/2024 test version without suppression

### Packages and Import ####################################################################

#   setwd
#    setwd(filepath)

d_and_f_folder <- Sys.getenv("folder_rtds_dose_and_fractionation")

#	  load package
    library("tidyverse")
    library("data.table")
    library(dplyr)

#	  import the extract 
    RTDS_EXTRACT <- 
      fread(
        file = d_and_f_folder, 
        sep = "|",
        showProgress = TRUE,
        data.table = TRUE
      )
    
## JA - mapping EOA trust codes

    RTDS_EXTRACT$COUNTRY <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER == 'E0A','England',RTDS_EXTRACT$COUNTRY)
    RTDS_EXTRACT$PROVIDER_NAME <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER == 'E0A','University Hospitals Sussex NHS Foundation Trust',RTDS_EXTRACT$PROVIDER_NAME)
    RTDS_EXTRACT$RT_NETWORK_NAME <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER == 'E0A','West London, North London, Surrey and Sussex',RTDS_EXTRACT$RT_NETWORK_NAME)
    RTDS_EXTRACT$CANCER_ALLIANCE_OLD <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER == 'E0A','Surrey and Sussex',RTDS_EXTRACT$CANCER_ALLIANCE_OLD)
    RTDS_EXTRACT$CANCER_ALLIANCE_NEW <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER == 'E0A','Surrey and Sussex',RTDS_EXTRACT$CANCER_ALLIANCE_NEW)
    RTDS_EXTRACT$ORGCODEPROVIDER <- ifelse(RTDS_EXTRACT$ORGCODEPROVIDER %in% c('E0A','RXH'),'RYR',RTDS_EXTRACT$ORGCODEPROVIDER) # do this one last as we need it to identify the others
    

###	Data Cleaning #######################################################################
#   drop Wales data
    
    
    RTDS_EXT =	RTDS_EXTRACT[COUNTRY == 'England',]

#   format dates and filter for the end date restriction:
    RTDS_EXT$CALENDAR_MONTH		= paste0(RTDS_EXT$CALENDAR_MONTH,'/01')
    RTDS_EXT$CALENDAR_MONTH		= as.Date(RTDS_EXT$CALENDAR_MONTH, format = "%Y/%m/%d")
    
    
    #filter if latest month is below 90% complete
    RTDS_EXT = filter(RTDS_EXT, CALENDAR_MONTH <= "2024-01-01 GMT")
    
    #   separate year and month columns
    RTDS_EXT$EPI_YEAR 		= year(RTDS_EXT$CALENDAR_MONTH)
    RTDS_EXT$EPI_MONTH 		= month(RTDS_EXT$CALENDAR_MONTH)
    
    unique(RTDS_EXT$EPI_MONTH)
    
    unique(RTDS_EXT$EPI_YEAR)
    
    unique(RTDS_EXT$CALENDAR_MONTH)
    
    #unique(WORKING_DAYS$CALENDAR_MONTH)
    
    #Add working days from spread sheet
    #SHOULD CHANGE THIS SO WE DON'T HAVE TO RELY ON A SPREADSHEET
    WORKING_DAYS<-read.csv(glue::glue("{d_and_f_folder}/WORKING_DAYS.csv"))
    
    
    WORKING_DAYS=unique(data.table(WORKING_DAYS[,1:2]))
    
    class(RTDS_EXT$CALENDAR_MONTH)
    class(WORKING_DAYS$CALENDAR_MONTH)
    
    unique(WORKING_DAYS$CALENDAR_MONTH)
    
    WORKING_DAYS$CALENDAR_MONTH = as.Date(WORKING_DAYS$CALENDAR_MONTH)
   
    
    #filter working_days to match main extract to avoid nulls
    WORKING_DAYS = filter(WORKING_DAYS, CALENDAR_MONTH < "2024-02-01 GMT")
    
    unique(WORKING_DAYS$CALENDAR_MONTH)
    
    RTDS_EXT<-merge(RTDS_EXT,WORKING_DAYS, by="CALENDAR_MONTH",all = TRUE)
    
    
    #View(RTDS_EXT)

    #Check for nulls
    test_wd <- RTDS_EXT %>% 
      filter(is.na(RTDS_EXT$EPI_YEAR))
    
    View(test_wd)
    
    
    #Intent
    
    RTDS_EXT$INTENT = recode(RTDS_EXT$INTENT 
                             , "PALLIATIVE"     = "Palliative"
                             , "RADICAL"        = "Curative"
                             , "OTHER/UNKNOWN"  = "Other/unknown"  )

    unique(RTDS_EXT$INTENT)
    
    #Code demographic measures as SACT
    
    #Age group 
    RTDS_EXT$AGEGROUP = recode(RTDS_EXT$AGE 
                             , "< 40"     = "<50"
                             , "40 - 49"  = "<50" 
                             , "50 - 59"  = "50-59"
                             , "60 - 69"  = "60-69"
                             , "70 - 79"  = "70-79" 
                             , "80 - 89"  = "80+" 
                             , "90+"      = "80+" )
    
    unique(RTDS_EXT$AGEGROUP)
    
   
    #Gender
    unique(RTDS_EXT$SEX)
    
    RTDS_EXT$GENDER = recode(RTDS_EXT$SEX 
                             , "1"     = "Male"
                             , "2"     = "Female"
                             , "0"     = "Other")
    
    unique(RTDS_EXT$GENDER)
    
    #Exclude other genders
    RTDS_EXT = filter(RTDS_EXT, !(GENDER =="Other"))
    
    unique(RTDS_EXT$ETHNICITYNAME)
    #Ethnicity, code to groups as SACT
    RTDS_EXT$ETHNICITY = recode(RTDS_EXT$ETHNICITYNAME
                                , "WHITE BRITISH" = "White"
                                , "ANY OTHER ASIAN BACKGROUND" = "Asian"
                                , "ASIAN INDIAN" = "Asian"
                                , "CHINESE" = "Asian"
                                , "ASIAN BANGLADESHI"  = "Asian"
                                , "ANY OTHER BLACK BACKGROUND" = "Black"
                                , "BLACK AFRICAN" = "Black"
                                , "BLACK CARIBBEAN" = "Black"
                                , "ASIAN INDIAN" = "Asian"
                                , "ASIAN PAKISTANI"  = "Asian"
                                , "MIXED WHITE AND BLACK CARIBBEAN" = "Mixed/Other"
                                , "ANY OTHER ETHNIC GROUP" = "Mixed/Other"
                                , "ANY OTHER MIXED BACKGROUND" = "Mixed/Other"
                                , "MIXED WHITE AND BLACK AFRICAN" = "Mixed/Other"
                                , "MIXED WHITE AND ASIAN" = "Mixed/Other"
                                , "Polish" = "White"
                                , "WHITE" = "White"
                                , "WHITE IRISH" = "White"
                                , "ANY OTHER WHITE BACKGROUND" = "White"
                                , "English" = "White"
                                , "NOT STATED" = "Unknown"
                                , "NOT KNOWN" = "Unknown")
  
  #Nulls to unknown  
  RTDS_EXT$ETHNICITY = ifelse(is.null(RTDS_EXT$ETHNICITY)|is.na(RTDS_EXT$ETHNICITY)|RTDS_EXT$ETHNICITY=="",
                                          "Unknown", RTDS_EXT$ETHNICITY)	
    
    
  unique(RTDS_EXT$ETHNICITY)
  
  #Deprivation
  unique(RTDS_EXT$IMD19_QUINTILE_LSOAS)
  
  #Nulls to postcode not matched as SACT
  RTDS_EXT$DEPRIVATION = ifelse(is.null(RTDS_EXT$IMD19_QUINTILE_LSOAS)|is.na(RTDS_EXT$IMD19_QUINTILE_LSOAS)|RTDS_EXT$IMD19_QUINTILE_LSOAS=="",
                              "Unknown", RTDS_EXT$IMD19_QUINTILE_LSOAS)	
  
 
  unique(RTDS_EXT$DEPRIVATION) 
  
  
  #ICBs
  unique(RTDS_EXT$ICB_HB)
  
  #Replace nulls with not matched
  RTDS_EXT$ICB = ifelse(is.null(RTDS_EXT$ICB_HB)|is.na(RTDS_EXT$ICB_HB)|RTDS_EXT$ICB_HB=="",
                        "Postcode not matched", RTDS_EXT$ICB_HB)	
  
  unique(RTDS_EXT$ICB)
  
  #Some patients treated in England live in Wales.  Recode to 'Postcode not matched'
  #New extract also patients in Scotland and Islands
  RTDS_EXT$ICB = recode(RTDS_EXT$ICB
                        , "Swansea Bay University Health Board" = "Postcode not matched"
                        , "Cwm Taf Morgannwg University Health Board" = "Postcode not matched"
                        , "Cardiff and Vale University Health Board" = "Postcode not matched"
                        , "Hywel Dda University Health Board" = "Postcode not matched"
                        , "Aneurin Bevan University Health Board" = "Postcode not matched"
                        , "Betsi Cadwaladr University Health Board" = "Postcode not matched"
                        , "Powys Teaching Health Board" = "Postcode not matched"
                        , "Forth Valley" = "Postcode not matched"
                        , "Lanarkshire" = "Postcode not matched"
                        , "Tayside" = "Postcode not matched"
                        , "Borders" = "Postcode not matched"
                        , "Lothian" = "Postcode not matched"
                        , "Grampian" = "Postcode not matched"
                        , "Sark Health Authority" = "Postcode not matched"
                        , "Highland" = "Postcode not matched"
                        , "Health & Social Care Board" = "Postcode not matched"
                        , "Fife" = "Postcode not matched"
                        , "Greater Glasgow and Clyde" = "Postcode not matched"
                        , "Guernsey Health Authority" = "Postcode not matched"
                        , "Dumfries and Galloway" = "Postcode not matched"
                        , "Jersey Health Authority" = "Postcode not matched"
                        , "Isle of Man" = "Postcode not matched"
  )
  
  #Format to match SACT
  RTDS_EXT$ICB = str_replace_all(RTDS_EXT$ICB, " Integrated Care Board", "")
  
  unique(RTDS_EXT$ICB)
  
  
  #Exclude unexpected gender/tumour
  RTDS_EXT = filter(RTDS_EXT, !(GENDER == "Female" & CANCERTYPE == "PROSTATE"))
  RTDS_EXT = filter(RTDS_EXT, !(GENDER == "Male" & CANCERTYPE == "CERVIX"))
  
  RTDS_EXT$CANCERTYPE = recode(RTDS_EXT$CANCERTYPE
                               ,"ANAL" = "Anal"
                               ,"BLADDER" = "Bladder"
                               ,"BRAIN" = "Brain"
                               ,"BREAST" = "Breast"
                               ,"CERVIX" = "Cervix"
                               ,"HEAD AND NECK" = "Head and Neck"
                               ,"LYMPHOMA" = "Lymphoma"
                               ,"LUNG" = "Lung"
                               ,"OESOPHAGUS" = "Oesophagus"
                               ,"PROSTATE" = "Prostate"
                               ,"RECTAL" = "Rectal"
                               ,"SKIN" = "Skin")
  
  unique(RTDS_EXT$CANCERTYPE)
  
  unique(RTDS_EXT$FRACTIONATION_CATEGORY)
  
  #Group to other as requested by RTDS team
  RTDS_EXT$FRACTIONATION_CATEGORY = recode(RTDS_EXT$FRACTIONATION_CATEGORY
                                           ,"other/non-curative fractionation" = "other")
  
  unique(RTDS_EXT$DOSE_FRACTIONATION_SCHEDULES)
  
  ###	All England Level Aggregations #######################################################################
  ##Assuming episodes only happen once and can be totalled up to make annual total
    
    #########New version
    ##Changed aggregation, so split Eng level by yr/month
  
  ENG_summarise_yr <- function(df,prim_lvl,prim_val,lvl_2,lvl_2_val) {
    
    prim_val    <- enquo(prim_val)
    lvl_2_val   <- enquo(lvl_2_val)
    
    
    df %>%
      group_by(
        activity_year = EPI_YEAR
        ,activity_month = NA
        ,period = "Year"
        ,wd = 0
        ,geo_lvl      = 'National'
        ,geo_val      = 'England' 
        ,prim_lvl     = prim_lvl
        ,prim_val     = !!prim_val
        ,lvl_2        = lvl_2
        ,lvl_2_val    = !!lvl_2_val ) %>%
      #summarise(episodeCount=sum(COUNT)) 
      summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
  }
  
  ENG_aggregation_yr <- rbind(
    
    ENG_summarise_yr(RTDS_EXT
                  ,"Total", NA
                  ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,NA  ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,"Age group" ,AGEGROUP)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Age group" ,AGEGROUP 
                   ,"Tumour group", CANCERTYPE)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Age group" ,AGEGROUP 
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,"Ethnicity" ,ETHNICITY)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Ethnicity" ,ETHNICITY
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,"Gender" ,GENDER)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Gender" ,GENDER
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,"Deprivation" ,DEPRIVATION)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Deprivation" ,DEPRIVATION
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                   ,"Tumour group", CANCERTYPE
                   ,"Treatment intent", INTENT)
    
    ,ENG_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                   ,"Treatment intent", INTENT
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Tumour group", CANCERTYPE
                   ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES)
    
    ,ENG_summarise_yr(RTDS_EXT
                   ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES
                   ,"Tumour group", CANCERTYPE)
    
    ,ENG_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                   ,"Fractionation category", FRACTIONATION_CATEGORY
                   ,NA ,NA)
    
    ,ENG_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                   ,"Tumour group", CANCERTYPE
                   ,"Fractionation category", FRACTIONATION_CATEGORY)
    
    ,ENG_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                   ,"Fractionation category", FRACTIONATION_CATEGORY
                   ,"Tumour group", CANCERTYPE)
    
  )
  
  #New version, as discussed with RTDS team, where under 5 group to 'other'
  
  ENG_aggregation_yr$prim_val <- ifelse((ENG_aggregation_yr$prim_lvl %in% c("Fractionation category", "Dose fractionation schedules") & ENG_aggregation_yr$episodeCount <5), 'other',ENG_aggregation_yr$prim_val)
  
  ENG_aggregation_yr$lvl_2_val <- ifelse((ENG_aggregation_yr$lvl_2 %in% c("Fractionation category", "Dose fractionation schedules") & ENG_aggregation_yr$episodeCount <5), 'other',ENG_aggregation_yr$lvl_2_val) 
  
  ENG_aggregation_yr = ENG_aggregation_yr%>% 
    group_by(
      activity_year, 		    
      activity_month,
      period,
      wd,
      geo_lvl,
      geo_val,
      prim_lvl,
      prim_val,
      lvl_2,
      lvl_2_val
      
    ) %>%
    summarise(episodeCount=sum(episodeCount))
  
  View(ENG_aggregation_yr)
  
  test_other <- ENG_aggregation_yr %>% 
    filter(prim_lvl == "Deprivation")
  View(test_other)
  
  
  #Monthly England data
    
    ENG_summarise <- function(df,prim_lvl,prim_val,lvl_2,lvl_2_val) {
      
      prim_val    <- enquo(prim_val)
      lvl_2_val   <- enquo(lvl_2_val)

      
      df %>%
        group_by(
          activity_year = EPI_YEAR
          ,activity_month = EPI_MONTH
          ,period = "Month"
          ,wd = WORKING_DAYS
          ,geo_lvl      = 'National'
          ,geo_val      = 'England' 
          ,prim_lvl     = prim_lvl
          ,prim_val     = !!prim_val
          ,lvl_2        = lvl_2
          ,lvl_2_val    = !!lvl_2_val ) %>%
        #summarise(episodeCount=sum(COUNT)) 
      summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
    }
    
    ENG_aggregation <- rbind(
      
      ENG_summarise(RTDS_EXT
                     ,"Total", NA
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,NA  ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,"Age group" ,AGEGROUP)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Age group" ,AGEGROUP 
                     ,"Tumour group", CANCERTYPE)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Age group" ,AGEGROUP 
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,"Ethnicity" ,ETHNICITY)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Ethnicity" ,ETHNICITY
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,"Gender" ,GENDER)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Gender" ,GENDER
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,"Deprivation" ,DEPRIVATION)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Deprivation" ,DEPRIVATION
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                     ,"Tumour group", CANCERTYPE
                     ,"Treatment intent", INTENT)
                    
      ,ENG_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                     ,"Treatment intent", INTENT
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Tumour group", CANCERTYPE
                     ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES)
      
      ,ENG_summarise(RTDS_EXT
                     ,"Dose fractionation schedules", DOSE_FRACTIONATION_SCHEDULES
                     ,"Tumour group", CANCERTYPE)
      
      ,ENG_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                     ,"Fractionation category", FRACTIONATION_CATEGORY
                     ,NA ,NA)
      
      ,ENG_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                     ,"Tumour group", CANCERTYPE
                     ,"Fractionation category", FRACTIONATION_CATEGORY)
      
      ,ENG_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative')]
                     ,"Fractionation category", FRACTIONATION_CATEGORY
                     ,"Tumour group", CANCERTYPE)
                    
      )
    
    #New version, as discussed with RTDS team, where under 5 group to 'other'
    
    ENG_aggregation$prim_val <- ifelse((ENG_aggregation$prim_lvl %in% c("Fractionation category", "Dose fractionation schedules") & ENG_aggregation$episodeCount <5), 'other',ENG_aggregation$prim_val)
    
    ENG_aggregation$lvl_2_val <- ifelse((ENG_aggregation$lvl_2 %in% c("Fractionation category", "Dose fractionation schedules") & ENG_aggregation$episodeCount <5), 'other',ENG_aggregation$lvl_2_val) 

    ENG_aggregation = ENG_aggregation%>% 
        group_by(
        activity_year, 		    
        activity_month,
        period,
        wd,
        geo_lvl,
        geo_val,
        prim_lvl,
        prim_val,
        lvl_2,
        lvl_2_val
        
      ) %>%
      summarise(episodeCount=sum(episodeCount))
      
    
    View(ENG_aggregation)
    
    #Test - still low numbers but not
    #Check deprivation for further breakdowns
    
    test_eng <- ENG_aggregation %>% 
      filter(episodeCount < 5)
    
    View(test_eng)
    
    test_eng2 <- test_eng %>%
      filter(prim_lvl %in% c("Fractionation category", "Dose fractionation schedules"))
    
    View(test_eng2)
    
    test_other <- ENG_aggregation %>% 
      filter(prim_lvl == "Deprivation")
    View(test_other)
   
###	Cancer Alliance Level Aggregations #######################################################################
  
    #New version adds aggregation by year
    #Separating month and year at CA level for low number adjustments
    
    
    CA_summarise_yr <- function(df,prim_lvl,prim_val) {
      
      prim_val <- enquo(prim_val)
      
      df %>%
        group_by(
          activity_year = EPI_YEAR
          ,activity_month = NA
          ,period = "Year"
          ,wd = 0
          ,geo_lvl      = 'Cancer Alliance'
          ,geo_val      = CANCER_ALLIANCE_NEW
          ,prim_lvl     = prim_lvl
          ,prim_val     = !!prim_val 
          ) %>%
        #summarise(episodeCount=sum(COUNT)) 
      summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
      }
  
    CA_aggregation_yr <-   rbind( 
      
                        CA_summarise_yr(RTDS_EXT
                                       ,"Total",NA)
                          
                        ,CA_summarise_yr(RTDS_EXT
                                      ,"Tumour group",CANCERTYPE)
                         
                        ,CA_summarise_yr(RTDS_EXT
                                      ,"Age group",AGEGROUP)
                        
                        ,CA_summarise_yr(RTDS_EXT
                                      ,"Gender",GENDER)
                        
                        ,CA_summarise_yr(RTDS_EXT
                                         ,"Ethnicity",ETHNICITY)
                          
                        ,CA_summarise_yr(RTDS_EXT
                                      ,"Deprivation",DEPRIVATION)
                         
                        ,CA_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                                      ,"Treatment intent",INTENT)
                          
                        )

       
    unique(CA_aggregation_yr$prim_val)
    
    
    #Test, lots of low numbers
    test_ca_yr <- CA_aggregation_yr %>% 
      filter(episodeCount < 5)
    
    
    View(test_ca_yr)
    
   
  
     ########Original version CA aggregation by months###############################################
    
    
    CA_summarise <- function(df,prim_lvl,prim_val) {
      
      prim_val <- enquo(prim_val)
      
      df %>%
        group_by(
          activity_year = EPI_YEAR
          ,activity_month = EPI_MONTH
          ,period = "Month"
          ,wd = WORKING_DAYS
          ,geo_lvl      = 'Cancer Alliance'
          ,geo_val      = CANCER_ALLIANCE_NEW
          ,prim_lvl     = prim_lvl
          ,prim_val     = !!prim_val 
        ) %>%
        #summarise(episodeCount=sum(COUNT)) 
        summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
    }
    
    CA_aggregation <-   rbind( 
      
      
      CA_summarise(RTDS_EXT
                    ,"Total",NA)
      
      ,CA_summarise(RTDS_EXT
                    ,"Tumour group",CANCERTYPE)
      
      ,CA_summarise(RTDS_EXT
                    ,"Age group",AGEGROUP)
      
      ,CA_summarise(RTDS_EXT
                    ,"Gender",GENDER)
      
      ,CA_summarise(RTDS_EXT
                    ,"Ethnicity",ETHNICITY)
      
      ,CA_summarise(RTDS_EXT
                    ,"Deprivation",DEPRIVATION)
      
      ,CA_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                    ,"Treatment intent",INTENT)
      
          )
    
       
    unique(CA_aggregation$prim_lvl)
    
    #Test low numbers as expected
    test_ca <- CA_aggregation %>% 
      filter(episodeCount < 5)
    
    View(test_ca)
    
    
    ###	ICB Level Aggregations #######################################################################
    
    #New version adds aggregation by year

    
    ICB_summarise_yr <- function(df,prim_lvl,prim_val) {
      
      prim_val <- enquo(prim_val)
      
      df %>%
        group_by(
          activity_year = EPI_YEAR
          ,activity_month = NA
          ,period = "Year"
          ,wd = 0
          ,geo_lvl      = 'Integrated Care Board'
          ,geo_val      = ICB
          ,prim_lvl     = prim_lvl
          ,prim_val     = !!prim_val 
        ) %>%
        #summarise(episodeCount=sum(COUNT)) 
      summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
    }
    
    ICB_aggregation_yr <-   rbind( 
      
      ICB_summarise_yr(RTDS_EXT
                      ,"Total",NA)
      
      ,ICB_summarise_yr(RTDS_EXT
                       ,"Tumour group",CANCERTYPE)
      
      ,ICB_summarise_yr(RTDS_EXT
                       ,"Age group",AGEGROUP)
      
      ,ICB_summarise_yr(RTDS_EXT
                       ,"Gender",GENDER)
      
      ,ICB_summarise_yr(RTDS_EXT
                       ,"Ethnicity",ETHNICITY)
      
      ,ICB_summarise_yr(RTDS_EXT
                        ,"Deprivation",DEPRIVATION)
      
      ,ICB_summarise_yr(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                       ,"Treatment intent",INTENT)
      
          )
    
    
    
    #Remove records with no ICB as postcode not matched
    ICB_aggregation_yr =filter(ICB_aggregation_yr, !(geo_val %in% "Postcode not matched"))
    
    
    #Test, lots of low numbers
    test_icb_yr <- ICB_aggregation_yr %>% 
      filter(episodeCount < 5)
    
    
    View(test_icb_yr)
    
  
    ########ICB Months##########################################
    
      
    ICB_summarise <- function(df,prim_lvl,prim_val) {
      
      prim_val <- enquo(prim_val)
      
      
      df %>%
        group_by(
          activity_year = EPI_YEAR
          ,activity_month = EPI_MONTH
          ,period = "Month"
          ,wd = WORKING_DAYS
          ,geo_lvl      = 'Integrated Care Board'
          ,geo_val      = ICB
          ,prim_lvl     = prim_lvl
          ,prim_val     = !!prim_val 
        ) %>%
        #summarise(episodeCount=sum(COUNT)) 
        summarise(episodeCount=n_distinct(RADIOTHERAPYEPISODEID ))
    }
    
    ICB_aggregation <-   rbind( 
      
      ICB_summarise(RTDS_EXT
                   ,"Total",NA)
      
      ,ICB_summarise(RTDS_EXT
                        ,"Tumour group",CANCERTYPE)
      
      ,ICB_summarise(RTDS_EXT
                        ,"Age group",AGEGROUP)
      
      ,ICB_summarise(RTDS_EXT
                        ,"Gender",GENDER)
      
      ,ICB_summarise(RTDS_EXT
                        ,"Ethnicity",ETHNICITY)
      
      ,ICB_summarise(RTDS_EXT
                        ,"Deprivation",DEPRIVATION)
      
      ,ICB_summarise(RTDS_EXT[RTDS_EXT$INTENT %in% c('Curative', 'Palliative')]
                        ,"Treatment intent",INTENT)
      
    )
    
    
    
    #Exclude patients without ICB
    ICB_aggregation =filter(ICB_aggregation, !(geo_val %in% "Postcode not matched"))
    
 
    test_icb <- ICB_aggregation %>% 
      filter(episodeCount < 5)
    
    View(test_icb)
    
    test_intent <- test_icb %>% 
      filter(prim_lvl == "Treatment intent")
    
    View(test_intent)
    
###	Gather and Export ##########################################################################################  
  
    RTDSdata = data.table(rbind(ENG_aggregation, ENG_aggregation_yr, CA_aggregation, CA_aggregation_yr, ICB_aggregation, ICB_aggregation_yr))
    rm(ENG_summarise, ENG_aggregation, ENG_aggregation_yr, CA_summarise, CA_aggregation, CA_summarise_yr, CA_aggregation_yr,  ICB_aggregation, ICB_aggregation_yr)
    RTDSdata$prim_val <- as.factor(RTDSdata$prim_val)
    RTDSdata$activity_year <- as.factor(RTDSdata$activity_year)
    RTDSdata$activity_month <- as.factor(RTDSdata$activity_month)
    
    #working days calculation
    RTDSdata$adjusted <- ifelse (RTDSdata$wd==0, RTDSdata$episodeCount, (RTDSdata$episodeCount/RTDSdata$wd)*21)
    RTDSdata$adjusted <- round(RTDSdata$adjusted, digits = 0)
  
    
    View(RTDSdata)
    
    unique(RTDSdata$wd)
    
    test_other <- RTDSdata %>% 
      filter(prim_lvl == "Deprivation" & prim_val == "Unknown")
    View(test_other)
    
    
    #   Save RDA for RSHINY app -  save paths dynamically renames with system date
    save(RTDSdata, file= paste0(d_and_f_folder,"/RTDSaggregation",as.character(Sys.Date()),".Rda"))

#   save csv copy for small number review
   fwrite	(RTDSdata,	file= paste0(d_and_f_folder,"/RTDSaggregation",as.character(Sys.Date()),".csv"),append=FALSE)

    