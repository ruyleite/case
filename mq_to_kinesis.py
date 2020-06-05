import pymqi
import datetime
import boto3
import json

#Connect with MQSeries

queue_manager = "QUEUE_MANAGER_NAME"
channel = "SVRCONN.1"
host = "mq.meudominio.com"
port = "1434"
conn_info = "%s(%s)" % (host, port)

qmgr = pymqi.QueueManager(None)
qmgr.connectTCPClient(queue_manager, pymqi.cd(), channel, conn_info)
getQ = pymqi.Queue(qmgr, 'QNAME')

#Connect with Kinesis
kinesis = boto3.client('kinesis')

fieldspecs = [
    ('card_number', 0, 16),
    ('tran_datetime', 16, 14),
    ('value', 30, 15),
    ('pdv_id', 45, 10),
    ('status', 56, 1),
]

json_field = []

for field in fieldspecs:
    json_field.append(field[0])

while 1 == 1:
    message_mg = getQ.get()

    message = {}
    value = ''

    for field in fieldspecs:
        value = message_mg[field[1]:(field[1] + field[2])].rstrip()

        if field[0] == 'value':
            value = int(value) / 100.0

        if field[0] == 'tran_datetime':
            value = datetime.datetime.strptime(value, '%Y%m%d%H%M%S')

        message[field[0]] = value

    kinesis.put_record(
        StreamName="InputStream",
        Data=json.dumps(message),
        PartitionKey="partitionkey")

