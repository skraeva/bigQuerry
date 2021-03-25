-- https://console.cloud.google.com/bigquery?sq=554952209762:c24ac3196a524582869bfb8c532cace1
-- Declare the variable to be used.
with variables as(  
  select '20201101' as date1, '20201207' as date2 -- Диапазон дат

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
   
     
        
    ,hits.item.productSku                   as SKU                         --SKU Продукта
    ,hits.transaction.transactionId         as transactionId               --transactionId
    ,hits.item.productName                  as productName
    ,channelGrouping,
                   
                    device.deviceCategory as deviceCategory
    
    FROM
    `movavi---owox-demo.969794.ga_sessions_*`,     --Формат ГГГГММДД
    UNNEST(hits) AS hits
    ,variables
        
    WHERE  
    _TABLE_SUFFIX BETWEEN date1 AND date2          			--Даты эксперимента
    
   
 
   
    Order by 1, hit_time
)




---------------------------------------------------------------------------------------------------------------------------
----------Строим воронку c BuyNow
---------------------------------------------------------------------------------------------------------------------------


, ga_table_joined as 
(

    SELECT distinct 
    user_1 as Users
     ,date_1
   ,DateValue
 ,transactionId_5 as Transactions
 ,Revenue_5 as Revenue

    FROM
    (
        SELECT *
        FROM

        (
            
            SELECT 
            user                        as user_1
            ,date                       as date_1
            ,hit_time

            FROM 
            ga_table ga
            
       where page_2  in (select page from `movavi---owox-demo.productGroups.contentGroups` where contentGroup in ('PE','PEM','PF','PFM','PM','PMM','PDN','PDNM')) and
     
    deviceCategory = 'desktop'
    
    and hit_time = (select min(hit_time) from ga_table where 
  --  page_2 = ga.page_2
    page_2  in (select page from `movavi---owox-demo.productGroups.contentGroups` where contentGroup in ('PE','PEM','PF','PFM','PM','PMM','PDN','PDNM'))  
    and user = ga.user)
    
          
        )  as Step_1
        
  
        
        LEFT JOIN
        
        (
            -- Transactions из ga_sessions
            SELECT 
            user
            ,date
            ,hit_time
            ,transactionId     as  transactionId_4
       
            
            FROM ga_table
            WHERE 
            hits_type = 'ITEM'
           
  
        ) as Step_4
    
        ON
        Step_1.user_1 = Step_4.user AND Step_1.hit_time <= Step_4.hit_time           

    ) as Step_1234
    
    LEFT JOIN
    

    
    (
    SELECT DateValue, transactionId_5, SUM(Revenue_5) as Revenue_5 FROM (
        -- Деньги из таблички Дианы
        SELECT Distinct --Distinct нужен из-за дублирования строчек по транзакциям
        DateValue
        
        ,Ref                               as transactionId_5
        ,case 
          WHEN Gross_USD_REFUND <> 0 
            then 
              case WHEN Gross_USD_COMPLETE = 0 THEN Gross_USD_COMPLETE
                  Else Gross_USD_COMPLETE + Gross_USD_REFUND
              END
            ELSE NET_USD_COMPLETE
          END AS Revenue_5
        ,Gross_USD_REFUND                as Refund_5
        ,Promotion_Coupon_Code
        
        FROM 
          `movavi---owox-demo.CUBE_Reference.v_Transaction_Cost_Product`,variables
          where ProductGroup in ('PE','PEM', 'PE Bundles', 'PE Content', 'PEM Bundles', 'PM Bundles', 'PMM Bundles', 'PM','PMM','PF','PFM','PDN','PDN')
        --  and PaySystem = 'Avangate'
     ) GROUP BY 1, 2

       
    ) as Step_5

    ON
    Step_1234.transactionId_4 = Step_5.transactionId_5        --Одна и та же транзакция
    
  
) 

SELECT distinct Date_1, DateValue,
transactions, 
Revenue
FROM ga_table_joined
where DateValue is not null
