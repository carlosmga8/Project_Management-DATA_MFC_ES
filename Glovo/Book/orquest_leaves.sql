 SELECT 

    case    when i.product_id = '15-C'          OR i.product_id =  'BCNF1'  then        'BCN'
            When i.product_id = '16-C'          OR i.product_id =  'BCNF2'  then        'BCN'
            When i.product_id = '325-C'         OR i.product_id =  'BCNF3'  then        'BCN'
            When i.product_id = '871-C'         OR i.product_id =  'BCNF4'  then        'BCN'
            When i.product_id = '2368-C'        OR i.product_id =  'BCNF5'  then        'BCN'
            When i.product_id = '4607-C'        OR i.product_id =  'BCNF11' then        'BCN'
            When i.product_id = '4398-C'        OR i.product_id =  'VALF2'  then        'VAL'
            When i.product_id = '4311-C'        OR i.product_id =  'MALF1'  then        'MAL'
            When i.product_id = '4275-C'        OR i.product_id =  'PALF1'  then        'PAL'
            When i.product_id = '4304-C'        OR i.product_id =  'SEVF1'  then        'SEV'
            When i.product_id = '5013-C'        OR i.product_id =  'MADF11' then        'MAD'
            When i.product_id = '3-C'           OR i.product_id =  'MADF2'  then        'MAD'
            When i.product_id = '1854-C'        OR i.product_id =  'MADF3'  then        'MAD'
            When i.product_id = '3172-C'        OR i.product_id =  'MADF4'  then        'MAD'
            When i.product_id = '4608-C'        OR i.product_id =  'MADF8'  then        'MAD'
            When i.product_id = '5012-C'        OR i.product_id =  'MADF10' then        'MAD'
            When i.product_id = '4399-C'        OR i.product_id =  'ZARF1'  then        'ZAR'
            When i.product_id = '1111-C'        then        'BCN'
            When i.product_id = '1302-C'        then        'MAD'
            When i.product_id = '0909-C'        then        'EXP' end as city,

    case    when i.product_id = '15-C'          OR i.product_id =  'BCNF1'  then        'BCNF1 - Girona'
            When i.product_id = '16-C'          OR i.product_id =  'BCNF2'  then        'BCNF2 - Consell de Cent'
            When i.product_id = '325-C'         OR i.product_id =  'BCNF3'  then        'BCNF3 - Francesc Carbonell'
            When i.product_id = '871-C'         OR i.product_id =  'BCNF4'  then        'BCNF4 - Marina'
            When i.product_id = '2368-C'        OR i.product_id =  'BCNF5'  then        'BCNF5 - Travessera de Dalt'
            When i.product_id = '4607-C'        OR i.product_id =  'BCNF11' then        'BCNF11 - Passatge Miner'
            When i.product_id = '4398-C'        OR i.product_id =  'VALF2'  then        'VALF2 - Centelles'
            When i.product_id = '4311-C'        OR i.product_id =  'MALF1'  then        'MALF1 - Cataluña'
            When i.product_id = '4275-C'        OR i.product_id =  'PALF1'  then        'PALF1 - Abu Yahya'
            When i.product_id = '4304-C'        OR i.product_id =  'SEVF1'  then        'SEVF1 - Juglar'
            When i.product_id = '5013-C'        OR i.product_id =  'MADF11' then        'MADF11 - Almagro Trinidad'
            When i.product_id = '3-C'           OR i.product_id =  'MADF2'  then        'MADF2 - Duque de Sesto'
            When i.product_id = '1854-C'        OR i.product_id =  'MADF3'  then        'MADF3 - Ronda de Toledo'
            When i.product_id = '3172-C'        OR i.product_id =  'MADF4'  then        'MADF4 - Suero de Quiñones'
            When i.product_id = '4608-C'        OR i.product_id =  'MADF8'  then        'MADF8 - Miguel Fleta'
            When i.product_id = '5012-C'        OR i.product_id =  'MADF10' then        'MADF10 - Chamartin Albendiego'
            When i.product_id = '4399-C'        OR i.product_id =  'ZARF1'  then        'ZARF1 - Domenech'
            When i.product_id = '1111-C'        then        'MFC - CL_BCN'
            When i.product_id = '1302-C'        then        'MFC - CL_MAD'
            When i.product_id = '0909-C'        then        'MFC - CL_EXP' end as product_id,

        i.employee_id,      
        i.incidence_type_name,
        cast(DATE_FORMAT(date(incidence_from_date), '%d/%m/%Y') as varchar) as incidence_from_date,
        cast(DATE_FORMAT(date(incidence_to_date), '%d/%m/%Y') as varchar) as incidence_to_date


    FROM 
        "sensitive_delta"."mfc_picker_management_odp"."incidences" i
    
    WHERE 
        i.business_id = 'GES' 
        AND incidence_type_name in ( 'Courier -Sick Leave','Courier - Parental leave','Courier - Paternity leave','Courier/Picker -Sick Leave')
        AND incidence_from_date >= date('2024-01-01') 
        AND employee_id = '132366824'
    order by 1,2,3,6
