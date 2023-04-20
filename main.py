import pyodbc
import random
import time
import json
from datetime import datetime
from azure.iot.device import IoTHubDeviceClient, Message





# Set up the connection parameters
server = 'deepanker'
database = 'master'
username = 'sa' 
password = '123456' 
driver = '{ODBC Driver 17 for SQL Server}' 


# Connect to the SQL Server
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
conn = pyodbc.connect(connection_string)
cursor = conn.cursor() #to store output result



# Connection string and message settings
CONNECTION_STRING = "HostName=DemoHubIotTeam.azure-devices.net;DeviceId=iothub;SharedAccessKey=WjMIv4i7AmDPcURa3ZZ9f4HL17mByKU0v/1g8h1/h1A="
MSG_INTERVAL = 60  # seconds

# Create IoT Hub client instance
device_client = IoTHubDeviceClient.create_from_connection_string(CONNECTION_STRING)

# Initialize message ID counter
message_id = 0


while True:
    
    message_id += 1

   
    # device_id = f"device-{message_id}"
    device_id="dev3368"
    data_produced_time = datetime.utcnow().isoformat()
    temperature = round(random.uniform(20, 30), 2)
    humidity = round(random.uniform(50, 70), 2)

    # Create message payload
    message_payload = {
        "messageId": message_id,
        "deviceId": device_id,
        "timeProduced": data_produced_time,
        "temperature": temperature,
        "humidity": humidity
    }

    # Create message instance
    message = Message(json.dumps(message_payload))

    # Send message to IoT Hub
    device_client.send_message(message)
    print("Message sent: {}".format(message_payload))


    # Insert query
    insert_query = "INSERT INTO master.dbo.customdata (messageid, deviceid, timeproduced,temperature,humidity) VALUES (?, ?, ?,?,?)" # Replace with your table name and column names



    # Values to be inserted
    values = ( message_id,device_id,datetime.now(),temperature,humidity ) # Replace with your actual values



    # Execute the insert query
    cursor.execute(insert_query, values)
    conn.commit()

    # Wait for specified interval before sending next message
    time.sleep(MSG_INTERVAL)


# Close the cursor and connection
cursor.close()
conn.close()
