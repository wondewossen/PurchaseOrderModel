  {{config( 
  --        materialized='incremental',
  --        incremental_strategy='merge',
  --        unique_key='Order' 
        )      
    }}    


with Get_PurchaseOrder as (
    select p.*,c.customername
    from SPARC_RAW.S4_GF.PURORDITEM p
        left outer join SPARC_RAW.S4_GF.CUSTOMER c
            on p.customer = c.customer

 --   {% if is_incremental() %}        
    --where (p.purchaseorder, p.purchaseorderitem, p.__timestamp) in
    --        (select purchaseorder,purchaseorderItem, max(__timestamp) as __timestamp
    --              from SPARC_RAW.S4_GF.PURORDITEM where purchaseorder in ('4500000000','4500000275') group by 1,2)
 --       where __timestamp > (select max(__create_datetime) from {{ this }})
 --   {% endif %}
),

Get_headerLevel as (
    select p.*, h.deletion_idn, h.ship_type_des
     from Get_PurchaseOrder as p  
        left outer join SPARC_BASE_WOND_DEMISSIE.PUBLIC.PURORD_SHIP h
            on p.purchaseorder = h.purchaseorder and ltrim(p.purchaseorderItem,'0') = h.purchaseorderItem
            where h.higherlvl = '0'
),

Get_Supplier as (
    select p.*, s.suppliername as supplier_name from Get_headerLevel p
        left outer join SPARC_RAW.S4_GF.SUPPLIER s
            on p.supplier = s.supplier
),

Get_Manufacturer as (
    select p.*,s.suppliername as Manufacturer_name from Get_Supplier p
        left outer join SPARC_RAW.S4_GF.SUPPLIER s
            on p.supplyingsupplier = s.supplier)

select 
        purchaseorder       as "Order",
        customer            as "Customer Number",
        customername        as "Buyer Name",
        supplier_name       as "Seller Name",
        supplier            as "Vendor Code",
        Manufacturer_name   as "Manufacturer Name",
        case when deletion_idn is null 
                then 'Open'
                else 'Canceled'
        end                 as "Order Status",
        podoctype           as "Order Type",
        shippingconditions  as "PO Priority",
        crmrefordernumber   as "Customer PO Ref. No",
        companycode         as "Buyer Code",
        incoterms1          as "Incoterm",
        incoterms2          as "Incoterm Location",
        termsofpaymentkey   as "Payment Term",
        changeddate         as "Modify Timestamp",
        ship_type_des       as "Shipment Method",
        __timestamp         as "create_datetime",
        count(*)            as "Item Count",
        sum(orderqty)       as "Order Total Qty",
        sum(netordervalue)  as "Order Total Amount",
        currencykey         as "Currency"
 from Get_Manufacturer 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21