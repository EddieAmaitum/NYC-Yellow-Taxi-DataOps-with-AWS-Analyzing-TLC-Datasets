import happybase # importing happybase library

# creating connection
connection = happybase.Connection('localhost', port=9090, autoconnect=False)

# opening connection to perform operations
def open_connection():
    connection.open()

# closing opened connection 
def close_connection():
    connection.close()

# getting the pointer to a table
def get_table(name):
    open_connection()
    table = connection.table(name)
    close_connection()
    return table

def batch_insert_data(filename, tablename): # batch insert data into the table
    print("starting batch insert of "+filename)
    file = open(filename, 'r')
    table = get_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=50000) as b:
        for line in file:
            if i!=0:
                temp = line.strip().split(",")
                b.put(temp[1]+temp[2] ,{'colfam1:VendorID': str(temp[0]), 'colfam1:tpep_pickup_datetime': str(temp[1]), 'colfam1:tpep_dropoff_datetime': str(temp[2]), 'colfam1:passenger_count': str(temp[3]), 'colfam1:trip_distance': str(temp[4]), 'colfam1:RatecodeID': str(temp[5]), 'colfam1:store_and_fwd_flag': str(temp[6]), 'colfam1:PULocationID': str(temp[7]), 'colfam1:DOLocationID': str(temp[8]), 'colfam1:payment_type': str(temp[9]),'colfam1:fare_amount': str(temp[10]), 'colfam1:extra': str(temp[11]), 'colfam1:mta_tax': str(temp[12]), 'colfam1:tip_amount': str(temp[13]), 'colfam1:tolls_amount': str(temp[14]), 'colfam1:improvement_surcharge': str(temp[15]), 'colfam1:total_amount': str(temp[16]), 'colfam1:congestion_surcharge': str(temp[17]), 'colfam1:airport_fee': str(temp[18]) })
            
            i+=1

    file.close()
    print("batch insert done")
    close_connection()    

batch_insert_data('yellow_tripdata_2017-03.csv', 'trip_records')
batch_insert_data('yellow_tripdata_2017-04.csv', 'trip_records')