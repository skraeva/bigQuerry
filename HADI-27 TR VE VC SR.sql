-- https://console.cloud.google.com/bigquery?sq=554952209762:7ccf7bab055345a6a690b3dd221d3d4f
-- Declare the variable to be used.
with variables as(
  -- Диапазон дат
--  select '20191206' as start_date, '20200115' as end_date
  select '20210224' as date1, '20210323' as date2


),

ga_table as 
(
    SELECT distinct 
    fullVisitorId                           as user                        --ID Юзера
    ,PARSE_DATE("%Y%m%d", date)             as date                        --Дата в формате ГГГГ-ММ-ДД, а не в формате ГГГГММДД
    ,visitStartTime + hits.time/1000        as hit_time                    --Временная отметка хита в секундах
    ,LOWER(hits.page.pagePath)              as page                        --Адрес страницы
    ,LOWER(
            REGEXP_REPLACE(
              REGEXP_REPLACE(hits.page.pagePath, r"\?.*|#.*|&.*|!.*|-([a-z]{2})$|index.html|.htm(l)?|.php", ""), r"^(secure|store|www|pdf|mac-cleaner).(avangate|movavi).(com|de|ru)/([a-z]{2}/)?", "/" 
              ) 
          )                                 as page_2                      --Обрезанный Адрес Страницы
    ,visitNumber                            as visitNumber                 --Номер сеанса пользователя
    ,hits.hitNumber                         as hitNumber                   --Номер хита
    ,hits.isExit                            as isExit                      --Последний хит в сессии
    ,hits.isEntrance                        as isEntrance                  --Первый хит в сессии
    
    ,hits.type                              as hits_type                   --Тип хита (пейдж, ивент, айтем и т.д.) Нужен для однозначного определения строчек с покупками
   ,geoNetwork.country                     as country
   ,device.deviceCategory as deviceCategory
                    
    ,trafficSource.medium as medium 
    ,trafficSource.source as source	
    
    FROM
    `movavi---owox-demo.969794.ga_sessions_*`,     --Формат ГГГГММДД
    UNNEST(hits) AS hits
    ,variables
        
    WHERE  
    _TABLE_SUFFIX BETWEEN date1 AND date2          			--Даты эксперимента
    and device.deviceCategory = 'desktop'
    
  /*  Union ALL
    
    SELECT distinct 
    fullVisitorId                           as user                        --ID Юзера
    ,PARSE_DATE("%Y%m%d", date)             as date                        --Дата в формате ГГГГ-ММ-ДД, а не в формате ГГГГММДД
    ,visitStartTime + hits.time/1000        as hit_time                    --Временная отметка хита в секундах
    ,LOWER(hits.page.pagePath)              as page                        --Адрес страницы
    ,LOWER(
            REGEXP_REPLACE(
              REGEXP_REPLACE(hits.page.pagePath, r"\?.*|#.*|&.*|!.*|-([a-z]{2})$|index.html|.htm(l)?|.php", ""), r"^(secure|store|www|pdf|mac-cleaner).(avangate|movavi).(com|de|ru)/([a-z]{2}/)?", "/" 
              ) 
          )                                 as page_2                      --Обрезанный Адрес Страницы
    ,visitNumber                            as visitNumber                 --Номер сеанса пользователя
    ,hits.hitNumber                         as hitNumber                   --Номер хита
    ,hits.isExit                            as isExit                      --Последний хит в сессии
    ,hits.isEntrance                        as isEntrance                  --Первый хит в сессии
    
    ,hits.type                              as hits_type                   --Тип хита (пейдж, ивент, айтем и т.д.) Нужен для однозначного определения строчек с покупками
   ,geoNetwork.country                     as country
    ,device.deviceCategory as deviceCategory
    ,trafficSource.medium as medium 
    ,trafficSource.source as source	
    
    FROM
    `movavi---owox-demo.8254485.ga_sessions_*`,     --Формат ГГГГММДД
    UNNEST(hits) AS hits
    ,variables
        
    WHERE  
    _TABLE_SUFFIX BETWEEN date1 AND date2          			--Даты эксперимента
    and device.deviceCategory = 'desktop'
 */
   
    Order by 1, hit_time
)

, TRVEVCSR as
(select * from `movavi---owox-demo.productGroups.contentGroups`
where contentGroup in ('VC', 'VCM', 'SR', 'SRM', 'VE', 'VEM', 'VE Plus', 'VEM Plus')
and domain is null)



---------------------------------------------------------------------------------------------------------------------------
----------Строим воронку c BuyNow
---------------------------------------------------------------------------------------------------------------------------


, ga_table_joined as 
(

    SELECT distinct 
    date_1 as date,
    contentGroup, 
    contentGroup_gd, 
    contentGroup_inst,
--    page_2,
    user_1 as users,
    user_down as downloaders,
    user_inst as installers
 
    FROM
    (
       SELECT user_1, contentGroup, min(date_1) as date_1, min(hit_time) as hit_time FROM (
            SELECT 
            user                        as user_1
            ,date                       as date_1
            ,hit_time
         --   ,visitNumber
         --   ,hitNumber
         --   ,page_2
          --  ,medium
         --   ,source
         --   ,isExit
            ,(select contentGroup from TRVEVCSR where TRVEVCSR.page = g.page_2) as contentGroup
            

            FROM 
            ga_table g
            
           where page_2 in (select page from  TRVEVCSR where pageType = 'ProductPage')  
           and regexp_contains(page, r'movavi.com/tr/')
           and country = 'Turkey'
           and deviceCategory = 'desktop'
           and hit_time = (select min(hit_time) from ga_table where user = g.user and page_2 = g.page_2)
             ) group by 1,2
          
        )  as Step_1
        
       LEFT JOIN
        
        
        (           
            SELECT 
            user                        as user_down
            ,date                       as date_down
            ,hit_time  as hit_time_down
         --   ,visitNumber
         --   ,hitNumber
          --  ,isEntrance
          ,(select contentGroup from TRVEVCSR where TRVEVCSR.page = gd.page_2) as contentGroup_gd
            
          

            FROM 
            ga_table gd
            
            where page_2 in (select page from  TRVEVCSR where pageType = 'Download')  
            
           and regexp_contains(page, r'-tr')
      
          
        )  as Step_2
        
         ON
        Step_1.user_1 = Step_2.user_down and Step_1.hit_time <= Step_2.hit_time_down
      --  AND ((Step_1.visitNumber = Step_2.visitNumber AND Step_1.hitNumber = Step_2.hitNumber-1) OR (Step_1.visitNumber = Step_2.visitNumber-1 AND Step_1.isExit = TRUE AND Step_2.isEntrance = TRUE))
      
       LEFT JOIN
        
        (           
            SELECT 
            user                        as user_inst
            ,date                       as date_inst
            ,hit_time  as hit_time_inst
           -- ,visitNumber
            --,hitNumber
          --  ,isEntrance
          ,(select contentGroup from TRVEVCSR where TRVEVCSR.page = gin.page_2) as contentGroup_inst

            FROM 
            ga_table gin
            
            where page_2 in (select page from  TRVEVCSR where pageType = 'Install')  
            
         --  and regexp_contains(page, r'-tr')
      
          
        )  as Step_3
        
         ON
        Step_2.user_down = Step_3.user_inst and Step_2.hit_time_down <= Step_3.hit_time_inst
      --  AND ((Step_2.visitNumber = Step_3.visitNumber AND Step_2.hitNumber = Step_3.hitNumber-1) OR (Step_2.visitNumber = Step_3.visitNumber-1 AND Step_2.isExit = TRUE AND Step_3.isEntrance = TRUE))

     

  
    
  
) 


--РАСЧЁТ ВОРОНКИ ПО ГРУППАМ И СТРАНИЦАМ

select distinct
contentGroup, 
--page, -- можно убрать (тогда надо во всех джойнах тоже)
users, 
downloaders, 
installers from
(
(select 
contentGroup, 
--page_2 as page,
count(distinct users) as users
from ga_table_joined
group by 1)

LEFT JOIN

(select 
contentGroup_gd, 
--page_2,
count(distinct downloaders) as downloaders
from ga_table_joined
group by 1)

on contentGroup = contentGroup_gd 
--and page_2 = page

LEFT JOIN

(select 
contentGroup_inst, 
--page_2 as page_3, 
count(distinct installers) as installers
from ga_table_joined
group by 1)

on contentGroup = contentGroup_inst  
--and page = page_3
)
order by users desc


--РАСЧЁТ ВОРОНКИ ПО ДАТАМ И ГРУППАМ ПРОДУКТОВ (МОЖНО БЕЗ ГРУПП)

/*
select distinct date 
,count(distinct users) as users
,count(distinct downloaders) as downloaders
,count(distinct installers) as installers
from ga_table_joined
where contentGroup = 'VE Plus'
group by 1
order by 1
*/

