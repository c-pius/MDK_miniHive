select C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42 

select C.C_NAME, C.C_ADDRESS from CUSTOMER C where C.C_NATIONKEY=7

select distinct * from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY' 

select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY' 

select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and CUSTOMER.C_CUSTKEY=42

select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION, REGION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY

select distinct CUSTOMER.C_CUSTKEY from REGION, NATION, CUSTOMER where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY

select distinct * from ORDERS, CUSTOMER where ORDERS.O_ORDERPRIORITY='1-URGENT' and CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY

select distinct * from CUSTOMER,ORDERS,LINEITEM where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'

select distinct * from LINEITEM,ORDERS,CUSTOMER where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR'and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'
