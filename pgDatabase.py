import pandas as pd
from collections import defaultdict
from sqlalchemy.inspection import inspect
from databaseModel import *


def query_to_dict(rset):
    result = defaultdict(list)
    for obj in rset:
        instance = inspect(obj)
        for key, x in instance.attrs.items():
            result[key].append(x.value)
    return result

def checkEventType(event1, event2):
    try:
        # db.session = db.session(engine)
        # EventType = Base.classes.event_type
        temp = pd.DataFrame(query_to_dict(db.session.query(EventType).all()))
        if not temp.empty:
            maxId = max(temp['id'])
        else:
            maxId = 0

        if maxId:
            if temp.loc[temp['name'] == event1].empty:
                db.session.add(EventType(name=event1))
            if temp.loc[temp['name'] == event2].empty:
                db.session.add(EventType(name=event2))
        else:
            db.session.add(EventType(name=event1))
            db.session.add(EventType(name=event2))
        db.session.commit()
        db.session.close()
    except:
        return 'DATABASE ERROR: Something wrong with event_type table!'

def writeEventLog(gwNumber, eventType):
    if type(gwNumber) == int:
        gwNumber = '%014i' %gwNumber

    # Gateway = Base.classes.gateway
    # EventType = Base.classes.event_type
    # GatewayLog = Base.classes.gateway_log
    # db.session = db.session(engine)
    try:
        gwId = db.session.query(Device).filter(Device.serial_number == gwNumber).first().id
    except:
        db.session.close()
        return'DATABASE ERROR: GW number is wrong!'

    try:
        etId = db.session.query(EventType).filter(EventType.name == eventType).first().id
    except:
        db.session.close()
        return 'DATABASE ERROR: Event type is wrong!'
    # db.session.add(DeviceLog(event_type_id=etId, gateway_id=gwId))
    db.session.commit()
    db.session.close()
