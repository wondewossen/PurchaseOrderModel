
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with Get_PurchaseOrder as (
    select p.*,c.customername
    from DEV_WOND_DEMISSIE.PUBLIC.PURORDITEM p
        left outer join SPARC_RAW.S4_GF.CUSTOMER c
            on p.customer = c.customer
                where (p.purchaseorder, p.purchaseorderitem, p.__timestamp) in
                (select purchaseorder,purchaseorderItem, max(__timestamp) as __timestamp
                    from DEV_WOND_DEMISSIE.PUBLIC.PURORDITEM group by 1,2)
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
        purchaseorder       as "PO Order",
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
        crmrefordernumber   as "Customer PO Ref No",
        companycode         as "Buyer Code",
        incoterms1          as "Incoterm",
        incoterms2          as "Incoterm Location",
        termsofpaymentkey   as "Payment Term",
        changeddate         as "Modify Timestamp",
        ship_type_des       as "Shipment Method",
        __timestamp         as "Create Datetime",
        count(*)            as "Item Count",
        sum(orderqty)       as "Order Total Qty",
        sum(netordervalue)  as "Order Total Amount",
        currencykey         as "Currency"
 from Get_Manufacturer 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,21
select *
from Get_PurchaseOrder

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
