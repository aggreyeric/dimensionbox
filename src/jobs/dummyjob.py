from libs.utils import BASE_LAKE_PATH, RAW_DATA_PATH, hash_sha256
print(f"BASE_LAKE_PATH : {BASE_LAKE_PATH}  ")
print(f"RAW_DATA_PATH : {RAW_DATA_PATH}  ")


for i in range(9):
    str_ = "Hello" + str(i)
    print(hash_sha256(str_))

























#  |-- salesorderid: string (nullable = true)
#  |-- orderqty: string (nullable = true)
#  |-- salesorderdetailid: string (nullable = true)
#  |-- unitprice: string (nullable = true)
#  |-- specialofferid: string (nullable = true)
#  |-- modifieddate: string (nullable = true)
#  |-- rowguid: string (nullable = true)
#  |-- productid: string (nullable = true)
#  |-- unitpricediscount: string (nullable = true)

# root
#  |-- salesorderid: string (nullable = true)
#  |-- shipmethodid: string (nullable = true)
#  |-- billtoaddressid: string (nullable = true)
#  |-- modifieddate: string (nullable = true)
#  |-- rowguid: string (nullable = true)
#  |-- taxamt: string (nullable = true)
#  |-- shiptoaddressid: string (nullable = true)
#  |-- onlineorderflag: string (nullable = true)
#  |-- territoryid: string (nullable = true)
#  |-- status: string (nullable = true)
#  |-- orderdate: string (nullable = true)
#  |-- creditcardapprovalcode: string (nullable = true)
#  |-- subtotal: string (nullable = true)
#  |-- creditcardid: string (nullable = true)
#  |-- currencyrateid: string (nullable = true)
#  |-- revisionnumber: string (nullable = true)
#  |-- freight: string (nullable = true)
#  |-- duedate: string (nullable = true)
#  |-- totaldue: string (nullable = true)
#  |-- customerid: string (nullable = true)
#  |-- salespersonid: string (nullable = true)
#  |-- shipdate: string (nullable = true)
#  |-- accountnumber: string (nullable = true)