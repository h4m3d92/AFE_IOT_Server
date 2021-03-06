from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import Table, Column, MetaData, Binary, Integer, String, SmallInteger, LargeBinary, Boolean, DateTime, Computed, ForeignKey, Numeric, Enum
import datetime
import pandas as pd
from collections import defaultdict
from sqlalchemy.inspection import inspect
from databaseModel import *
import random, re

def query_to_dict(rset):
    result = defaultdict(list)
    for obj in rset:
        instance = inspect(obj)
        for key, x in instance.attrs.items():
            result[key].append(x.value)
    return result

Base = automap_base()
engine = create_engine("postgresql://postgres:5720022788@localhost/gw_db")
Base.prepare(engine, reflect=True)

def checkEventType(event1, event2):
    try:
        session = Session(engine)
        EventType = Base.classes.event_type
        temp = pd.DataFrame(query_to_dict(session.query(EventType).all()))
        if not temp.empty:
            maxId = max(temp['id'])
        else:
            maxId = 0

        if maxId:
            if temp.loc[temp['name'] == event1].empty:
                session.add(EventType(name=event1))
            if temp.loc[temp['name'] == event2].empty:
                session.add(EventType(name=event2))
        else:
            session.add(EventType(name=event1))
            session.add(EventType(name=event2))
        session.commit()
        session.close()
    except:
        return 'DATABASE ERROR: Something wrong with event_type table!'

def writeEventLog(gwNumber, eventType):
    if type(gwNumber) == int:
        gwNumber = '%014i' %gwNumber

    Gateway = Base.classes.gateway
    EventType = Base.classes.event_type
    GatewayLog = Base.classes.gateway_log
    session = Session(engine)
    try:
        gwId = session.query(Gateway).filter(Gateway.serial_number == gwNumber).first().id
    except:
        session.close()
        return'DATABASE ERROR: GW number is wrong!'

    try:
        etId = session.query(EventType).filter(EventType.name == eventType).first().id
    except:
        session.close()
        return 'DATABASE ERROR: Event type is wrong!'
    session.add(GatewayLog(event_type_id=etId, gateway_id=gwId))
    session.commit()
    session.close()

def readGateways():
    Gateway = Base.classes.gateway
    session = Session(engine)
    try:
        return pd.DataFrame(query_to_dict(session.query(Gateway).order_by(Gateway.id).all()))
    except:
        return 'DATABASE ERROR: Cannot connect to gateway table!'


# print(readGateways()['project_name'])

# for i in range(10):
#     writeEventLog(409900994215, 'gw_offline')
#     writeEventLog(409900994215, 'gw_online')
#     if not i%1000:
#         print(i)

# a = '491111111112'
# print(writeEventLog(a, 'gw_offline'))
# checkEventType()
