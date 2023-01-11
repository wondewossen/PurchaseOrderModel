  {{config( 
          materialized='incremental',
          unique_key=['Po_Number','Po_Item']
        )      
    }}    

with Get_PurchaseOrder as (
    select p.*,c.customername
    from SPARC_RAW.S4_GF.PURCHASE_ORD_ITEM p
        left outer join SPARC_RAW.S4_GF.CUSTOMER c
            on p.kunnr = c.customer
                where (p.EBELN, p.EBELP, p.__timestamp) in
                (select EBELN,EBELP, max(__timestamp) as __timestamp
                    from SPARC_RAW.S4_GF.PURCHASE_ORD_ITEM group by 1,2)
    {% if is_incremental() %}        
        and p.__timestamp >= (
            select case when (select max(Creation_ts) from {{ this }}) is null
                then (select min(__timestamp) from SPARC_RAW.S4_GF.PURCHASE_ORD_ITEM)
                else (select max(Creation_ts) from {{ this }})
                end
        )
    {% endif %}
),

Get_Supplier as (
    select p.*, s.suppliername as supplier_name from Get_PurchaseOrder p
        left outer join SPARC_RAW.S4_GF.SUPPLIER s
            on p.LIFNR = s.supplier
),

Get_Manufacturer as (
    select p.*,s.suppliername as Manufacturer_name from Get_Supplier p
        left outer join SPARC_RAW.S4_GF.SUPPLIER s
            on p.LLIEF = s.supplier)


select 
        EBELN                   as Po_Number,
        EBELP                   as Po_Item,
        BSART                   as Doc_Type,
        STATU_H                 as PO_Status,
        LOEKZ2                  as Deletion_Ind_Itm,
        matnr                   as Material_Number,
        LIFNR                   as Vendor_Number,
        Manufacturer_name       as Vendor_Name,
        LLIEF                   as Supplier_Number,  
        Manufacturer_name       as Supplier_Name,                            
        kunnr                   as Customer_Number,
        customername            as Customer_Name,
        PSTYP                   as Item_Category,
        WERKS                   as Plant,
        audat                   as Document_Date,
        bsgru                   as Ordering_Reason,
        bukrs                   as Company_Code,
        lgort                   as Storage_Location,
        aedat                   as Create_Date,
        LASTCHANGEDATETIME      as Changed_Timestamp,
        inco1                   as Incoterm1,
        inco2                   as Incoterm2,
        zterm                   as Payment_Term,
        SHIPCOND                as Shipping_Condition,
        SPE_CRM_REF_SO          as Crm_ref_number,
        ERNAM                   as Person_Created,
        PROCSTAT                as Proc_State,
        infnr                   as Info_Record,
        ABSKZ                   as Rejection_Ind,
        erekz                   as Final_Invoice_Ind,
        repos                   as Invoice_Receipt_Ind,
        EVERS                   as Shipping_Instruction,
        UEBPO                   as Higher_LevItem,
        MENGE                   as Base_Unit_Measure,
        waers                   as Currency,
        NETWR                   as Net_Order_Value,
        MENGE                   as Order_Quantity,
        __timestamp             as Creation_TS,
        current_timestamp       as load_datetime

 from Get_Manufacturer 
