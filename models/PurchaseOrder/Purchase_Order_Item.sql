  {{config( 
          materialized='incremental',
          unique_key=['POOrder','POItem']
        )      
    }}    

with Get_PurchaseOrder as (
    select p.*,c.customername
    from DEV_WOND_DEMISSIE.PUBLIC.PURORDITEM p
        left outer join SPARC_RAW.S4_GF.CUSTOMER c
            on p.customer = c.customer
                where (p.purchaseorder, p.purchaseorderitem, p.__timestamp) in
                (select purchaseorder,purchaseorderItem, max(__timestamp) as __timestamp
                    from DEV_WOND_DEMISSIE.PUBLIC.PURORDITEM group by 1,2)
    {% if is_incremental() %}        
        and p.__timestamp >= (
            select case when (select max(Createdatetime) from {{ this }}) is null
                then (select min(__timestamp) from DEV_WOND_DEMISSIE.PUBLIC.PURORDITEM)
                else (select max(Createdatetime) from {{ this }})
                end
        )
    {% endif %}
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
        purchaseorder       as POOrder,
        purchaseorderItem   as POItem,
        customer            as CustomerNumber,
        customername        as BuyerName,
        supplier_name       as SellerName,
        supplier            as VendorCode,
        Manufacturer_name   as ManufacturerName,
        case when deletion_idn is null 
                then 'Open'
                else 'Canceled'
        end                 as OrderStatus,
        podoctype           as OrderType,
        shippingconditions  as POPriority,
        crmrefordernumber   as CustomerPORefNo,
        companycode         as BuyerCode,
        incoterms1          as Incoterm,
        incoterms2          as IncotermLocation,
        termsofpaymentkey   as PaymentTerm,
        changeddate         as ModifyTimestamp,
        ship_type_des       as ShipmentMethod,
        __timestamp         as Createdatetime,
        count(*)            as ItemCount,
        sum(orderqty)       as OrderTotalQty,
        sum(netordervalue)  as OrderTotalAmount,
        currencykey         as Currency
 from Get_Manufacturer 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,22
