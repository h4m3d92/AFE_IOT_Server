# -*- encoding: utf-8 -*-
from flask import Flask
from flask_login import UserMixin
from sqlalchemy import Table, Column, MetaData, Binary, Integer, String, SmallInteger, LargeBinary, Boolean, DateTime, Computed, ForeignKey, Numeric, Enum,desc
# from geoalchemy2 import Geometry
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
# from app import db, login_manager
from datetime import *
import enum
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from sqlalchemy import create_engine
# from geoalchemy2 import Geometry
# import openpyxl
import json


with open("config.json") as json_data_file:
    configData = json.load(json_data_file)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://%s:%s@%s/%s" % (configData['postgres']['user'], configData['postgres']['password'], configData['postgres']['host'], configData['postgres']['db'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
migrate = Migrate(app, db)
metadata = MetaData() #schema="gw_schema"

# Assossiation Tables
GroupGateway= Table('a_group_gateway',db.Model.metadata,
    # Column('id', SmallInteger, primary_key=True),
    Column('group_id',SmallInteger, ForeignKey('group.id')),
    Column('gateway_id',SmallInteger, ForeignKey('gateway.id'))
    # schema='gw_schema'
    )

TaskGateway= Table('a_task_gateway',db.Model.metadata,
    # Column('id', SmallInteger, primary_key=True),
    Column('task_id',SmallInteger, ForeignKey('task.id')),
    Column('gateway_id',SmallInteger, ForeignKey('gateway.id'))
    # schema='gw_schema'
    )

RolePermission= Table('a_role_permission',db.Model.metadata,
    # Column('id', SmallInteger, primary_key=True),
    Column('role_id',SmallInteger, ForeignKey('role.id')),
    Column('permission_id',SmallInteger, ForeignKey('permission.id'))
    # schema='gw_schema'
    )

# Enum Classes
class RSEnum(enum.Enum):
    zero = 0
    one  = 1
    two  = 2


class Protocol(enum.Enum):
    mode_c = "MODE C"
    mode_e = "MODE E"
    dlms   = "DLMS"


class Manfr(enum.Enum):
    unknown = "Unknown"
    tap     = "TAP"
    tfc     = "TFC"
    eaa     = "EAA"
    bst     = "BST"
    hxe     = "HXE"
    abb     = "ABB"
    snh     = "SNH"

# User related Classes
class User(db.Model, UserMixin):
    __tablename__ = 'user'
    # __table_args__ = {'schema': 'gw_schema'}
    # metadata=metadata
    id = Column(SmallInteger, primary_key=True)
    username = Column(String(15), nullable=False, unique=True)
    password = Column(LargeBinary)
    first_name = Column(String(15))
    last_name = Column(String(15))
    is_active = Column(Boolean, unique=False, default=True)
    mobile = Column(String(11))
    email = Column(String(30))
    date_created = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    group_id = Column(SmallInteger, ForeignKey('group.id'))
    role_id = Column(SmallInteger, ForeignKey('role.id'))

    def __init__(self, **kwargs):
        for property, value in kwargs.items():
            # depending on whether value is an iterable or not, we must
            # unpack it's value (when **kwargs is request.form, some values
            # will be a 1-element list)
            if hasattr(value, '__iter__') and not isinstance(value, str):
                # the ,= unpack of a singleton fails PEP8 (travis flake8 test)
                value = value[0]

            # if property == 'password':
            #     value = hash_pass(value)  # we need bytes here (not plain str)

            setattr(self, property, value)

    def __repr__(self):
        return str(self.username)


class Group(db.Model, UserMixin):
    __tablename__ = 'group'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(30), nullable=False, unique=True)
    date_created = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    gateway_rel = relationship("Gateway", secondary=GroupGateway, backref='group')


class Permission(db.Model, UserMixin):
    __tablename__ = 'permission'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(SmallInteger, primary_key=True)
    content_id = Column(SmallInteger, ForeignKey('content.id'))
    p_view = Column(Boolean, unique=False, default=False)
    p_edit = Column(Boolean, unique=False, default=False)
    p_add = Column(Boolean, unique=False, default=False)
    p_delete = Column(Boolean, unique=False, default=False)
    access_right_id = Column(SmallInteger, ForeignKey('access_right.id'))
    role_rel = relationship("Role", secondary=RolePermission, backref='permission')


class AccessRight(db.Model, UserMixin):
    __tablename__ = 'access_right'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)


class Content(db.Model, UserMixin):
    __tablename__ = 'content'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(SmallInteger, primary_key=True)
    name = Column(String, nullable=False, unique=True)


class Role(db.Model, UserMixin):
    __tablename__ = 'role'
    # __table_args__ = {'schema': 'gw_schema'}
    # metadata=metadata
    id = Column(SmallInteger, primary_key=True)
    name = Column(String(15), nullable=False, unique=True)
    permission_rel = relationship("Permission", secondary=RolePermission, backref='role')


# Gateway Classes
class Gateway(db.Model, UserMixin):

    __tablename__  = 'gateway'
    # __table_args__ = {'schema': 'gw_schema'}

    id              = Column(SmallInteger, primary_key=True)
    project_name    = Column(String(30), nullable=False, unique=True)
    serial_number   = Column(String(14), nullable=False, unique=True)
    data_filter     = Column(Boolean, unique=False, default=False )
    last_comm_time  = Column(DateTime, nullable = True)
    is_edited       = Column(Boolean, unique=False, default=False)
    manfr           = Column(Enum(Manfr), nullable=False, default=Manfr.unknown)
    a_key           = Column(String(32), nullable=True, unique=False)
    e_key           = Column(String(32), nullable=True, unique=False)
    m_key           = Column(String(32), nullable=True, unique=False)
    password        = Column(String(16), nullable=False, unique=False)
    address         = Column(String, nullable=True, unique=False)
    # coordinates     = Column(Geometry('POINT'), nullable=True)    #sudo apt install postgis postgresql-12-postgis-3     and then     psql mydatabasename -c "CREATE EXTENSION postgis";      on the server terminal
    date_created    = Column(DateTime(timezone=False), server_default=func.now())
    date_modified   = Column(DateTime(timezone=False), server_default=func.now())
    group_rel       = relationship("Group", secondary=GroupGateway,backref='gateway')


class GatewayLog(db.Model, UserMixin):

    __tablename__ = 'gateway_log'
    # __table_args__ = {'schema': 'gw_schema'}

    id            = Column(Integer, primary_key=True)
    event_type_id = Column(SmallInteger, ForeignKey('event_type.id'), nullable=False)
    gateway_id    = Column(SmallInteger, ForeignKey('gateway.id'), nullable=False)
    ts            = Column(DateTime(timezone=False), server_default=func.now())


# Task and Event
class Task(db.Model, UserMixin):

    __tablename__ = 'task'
    # __table_args__ = {'schema': 'gw_schema'}

    id            = Column(SmallInteger, primary_key=True)
    name          = Column(String(30), nullable=False, unique=True)
    type_id       = Column(SmallInteger, ForeignKey('task_type.id'), nullable=False)
    execute_time  = Column(DateTime, nullable = False)
    date_created  = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    gateway_rel   = relationship("Gateway", secondary=TaskGateway, backref='task')


class TaskType(db.Model, UserMixin):

    __tablename__ = 'task_type'
    # __table_args__ = {'schema': 'gw_schema'}

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)


class EventType(db.Model, UserMixin):

    __tablename__ = 'event_type'
    # __table_args__ = {'schema': 'gw_schema'}

    id      = Column(SmallInteger, primary_key=True)
    name    = Column(String(30), nullable=False, unique=True)
    name_fa = Column(String(30), nullable=True, unique=True)


class EventLog(db.Model, UserMixin):

    __tablename__ = 'event_log'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(Integer, primary_key=True)
    timestamp    = Column(DateTime(timezone=False), server_default=func.now())
    event_type_id = Column(SmallInteger, ForeignKey('event_type.id'), nullable=False)
    user_id = Column(SmallInteger, ForeignKey('user.id'), nullable=False)


# Meter and Readout
class Meter(db.Model, UserMixin):

    __tablename__ = 'meter'
    # __table_args__ = {'schema': 'gw_schema'}
    # metadata=metadata
    id               = Column(SmallInteger, primary_key=True)
    name             = Column(String(30), nullable=False)
    serial_number    = Column(String(14), nullable=False)
    rs_dev_addr      = Column(String(14), nullable=False)
    rs_port_number   = Column(Enum(RSEnum), nullable=False, default=RSEnum.one)
    ramz_rayaneh     = Column(String(16), nullable=True)
    shenase_ghabz    = Column(String(16), nullable=True)
    shomare_parvande = Column(String(16), nullable=True)
    address          = Column(String(60), nullable=True)
    omur             = Column(String(20), nullable=True)
    manfr            = Column(Enum(Manfr), nullable=False, default=Manfr.unknown)
    protocol         = Column(Enum(Protocol), nullable=False, default=Protocol.mode_c)
    a_key            = Column(String(32), nullable=True, unique=False)
    m_key            = Column(String(32), nullable=True, unique=False)
    e_key            = Column(String(32), nullable=True, unique=False)
    date_created     = Column(DateTime(timezone=False), server_default=func.now())
    date_modified    = Column(DateTime(timezone=False), server_default=func.now())
    gateway_id       = Column(SmallInteger, ForeignKey('gateway.id'))
    customer_id      = Column(SmallInteger, ForeignKey('customer.id'))


class Customer(db.Model, UserMixin):
    __tablename__ = 'customer'
    # __table_args__ = {'schema': 'gw_schema'}

    id = Column(SmallInteger, primary_key=True)
    name = Column(String(30), nullable=False, unique=True)


class Readout(db.Model, UserMixin):

    __tablename__  = 'readout'
    # __table_args__ = {'schema': 'gw_schema'}

    id                = Column(Integer, primary_key=True)
    v_1               = Column(Numeric(6, 3))
    v_2               = Column(Numeric(6, 3))
    v_3               = Column(Numeric(6, 3))
    i_1               = Column(Numeric(6, 3))
    i_2               = Column(Numeric(6, 3))
    i_3               = Column(Numeric(6, 3))
    energy_ai         = Column(Numeric(11, 3))
    energy_ai_1       = Column(Numeric(11, 3))
    energy_ai_2       = Column(Numeric(11, 3))
    energy_ai_3       = Column(Numeric(11, 3))
    energy_ai_4       = Column(Numeric(11, 3))
    energy_ae         = Column(Numeric(11, 3))
    energy_ae_1       = Column(Numeric(11, 3))
    energy_ae_2       = Column(Numeric(11, 3))
    energy_ae_3       = Column(Numeric(11, 3))
    energy_ae_4       = Column(Numeric(11, 3))
    energy_aa         = Column(Numeric(11, 3))
    energy_aa_1       = Column(Numeric(11, 3))
    energy_aa_2       = Column(Numeric(11, 3))
    energy_aa_3       = Column(Numeric(11, 3))
    energy_aa_4       = Column(Numeric(11, 3))
    energy_ri         = Column(Numeric(11, 3))
    energy_re         = Column(Numeric(11, 3))
    power_a           = Column(Numeric(6, 3))
    power_factor      = Column(Numeric(4, 3))
    freq              = Column(Numeric(5, 3))
    overload_last_dt  = Column(String(20), nullable=True)
    magnet_dt         = Column(String(20), nullable=True)
    magnet_num        = Column(Numeric(4, 0))
    magnet_dur        = Column(String(10), nullable=True)
    max_dem           = Column(String(20), nullable=True)
    max_dem_dt        = Column(String(20), nullable=True)
    meter_date        = Column(String(10), nullable=True)
    meter_time        = Column(String(10), nullable=True)
    power_fail_dt     = Column(String(20), nullable=True)
    power_return_dt   = Column(String(20), nullable=True)
    power_fail_num    = Column(Numeric(4, 0))
    terminal_dt       = Column(String(20), nullable=True)
    terminal_num      = Column(Numeric(4, 0))
    battery           = Column(String(10))
    checksum          = Column(String(10))
    server_dt         = Column(DateTime(timezone=False), server_default=func.now())
    meter_id          = Column(SmallInteger, ForeignKey('meter.id'), nullable=False)
    readout_map_id = Column(Integer, ForeignKey('readout_map.id'), nullable=False)
    task_id = Column(SmallInteger, ForeignKey('task.id'), nullable=False)

class ReadoutMap(db.Model, UserMixin):

    __tablename__  = 'readout_map'
    # __table_args__ = {'schema': 'gw_schema'}

    id                     = Column(Integer, primary_key=True)
    meter_ident            = Column(String(50), nullable=True, unique=True)
    meter_serial_number_1  = Column(String(21), nullable=True)
    meter_serial_number_2  = Column(String(21), nullable=True)
    v_1                    = Column(String(21), nullable=True)
    v_2                    = Column(String(21), nullable=True)
    v_3                    = Column(String(21), nullable=True)
    i_1                    = Column(String(21), nullable=True)
    i_2                    = Column(String(21), nullable=True)
    i_3                    = Column(String(21), nullable=True)
    energy_ai              = Column(String(21), nullable=True)
    energy_ai_1            = Column(String(21), nullable=True)
    energy_ai_2            = Column(String(21), nullable=True)
    energy_ai_3            = Column(String(21), nullable=True)
    energy_ai_4            = Column(String(21), nullable=True)
    energy_ae              = Column(String(21), nullable=True)
    energy_ae_1            = Column(String(21), nullable=True)
    energy_ae_2            = Column(String(21), nullable=True)
    energy_ae_3            = Column(String(21), nullable=True)
    energy_ae_4            = Column(String(21), nullable=True)
    energy_aa              = Column(String(21), nullable=True)
    energy_aa_1            = Column(String(21), nullable=True)
    energy_aa_2            = Column(String(21), nullable=True)
    energy_aa_3            = Column(String(21), nullable=True)
    energy_aa_4            = Column(String(21), nullable=True)
    energy_ri              = Column(String(21), nullable=True)
    energy_re              = Column(String(21), nullable=True)
    power_a                = Column(String(21), nullable=True)
    power_factor           = Column(String(21), nullable=True)
    freq                   = Column(String(21), nullable=True)
    overload_last_dt       = Column(String(21), nullable=True)
    magnet_dt              = Column(String(21), nullable=True)
    magnet_num             = Column(String(21), nullable=True)
    magnet_dur             = Column(String(21), nullable=True)
    max_dem                = Column(String(21), nullable=True)
    max_dem_dt             = Column(String(21), nullable=True)
    meter_date             = Column(String(21), nullable=True)
    meter_time             = Column(String(21), nullable=True)
    power_fail_dt          = Column(String(21), nullable=True)
    power_return_dt        = Column(String(21), nullable=True)
    power_fail_num         = Column(String(21), nullable=True)
    terminal_dt            = Column(String(21), nullable=True)
    terminal_num           = Column(String(21), nullable=True)
    battery                = Column(String(21), nullable=True)
    checksum               = Column(String(21), nullable=True)
    server_dt              = Column(DateTime(timezone=False), server_default=func.now())

class ReadoutLog(db.Model, UserMixin):
    __tablename__ = 'readout_log'

    id              = Column(SmallInteger, primary_key=True)
    server_dt       = Column(DateTime(timezone=False), server_default=func.now())
    type_id         = Column(SmallInteger, ForeignKey('readout_type.id'), nullable=False)
    task_id         = Column(SmallInteger, ForeignKey('task.id'), nullable=False)
    meter_id        = Column(SmallInteger, ForeignKey('meter.id'), nullable=False)

class ReadoutType(db.Model, UserMixin):
    __tablename__ = 'readout_type'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)

# Statistics
class GatewayStats(db.Model, UserMixin):
    __tablename__ = 'gateway_stats'

    id                 = Column(Integer, primary_key=True)
    type_id           = Column(SmallInteger, ForeignKey('stats_type.id'), nullable=False)
    gateway_id         = Column(SmallInteger, ForeignKey('gateway.id'), nullable=False)
    total_meter        = Column(SmallInteger, nullable=False)   # total Nu of meters under gw
    succ_meter_last    = Column(SmallInteger, nullable=False)   # total Nu of meter that read successfully last time
    succ_meter_atleast = Column(SmallInteger, nullable=False)   # total Nu of meter that read successfully at least once
    total_read_daily   = Column(SmallInteger, nullable=False)   # total Nu of attempt on readout for the current day
    succ_read_daily    = Column(SmallInteger, nullable=False)   # total Nu of successful readout for the current day
    err1_read_daily    = Column(SmallInteger, nullable=False)   # total Nu of readout that is not acceptable for the current day
    err2_read_daily    = Column(SmallInteger, nullable=False)   # total Nu of readout which contains missing values for the current day
    err3_read_daily    = Column(SmallInteger, nullable=False)   # reserved
    date_created       = Column(DateTime(timezone=False), server_default=func.now())
    date_present       = Column(DateTime(timezone=False), server_default=func.now())

class StatsType(db.Model, UserMixin):
    __tablename__ = 'stats_type'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)

class readoutParameters():
    def __init__(self):
        self.meter_ident = ''
        self.meter_serial_number_1 = ''
        self.meter_serial_number_2 = ''
        self.v_1 = 0
        self.v_2 = 0
        self.v_3 = 0
        self.i_1 = 0
        self.i_2 = 0
        self.i_3 = 0
        self.energy_ai = 0
        self.energy_ai_1 = 0
        self.energy_ai_2 = 0
        self.energy_ai_3 = 0
        self.energy_ai_4 = 0
        self.energy_ae = 0
        self.energy_ae_1 = 0
        self.energy_ae_2 = 0
        self.energy_ae_3 = 0
        self.energy_ae_4 = 0
        self.energy_aa = 0
        self.energy_aa_1 = 0
        self.energy_aa_2 = 0
        self.energy_aa_3 = 0
        self.energy_aa_4 = 0
        self.energy_ri = 0
        self.energy_re = 0
        self.power_a = 0
        self.power_factor = 0
        self.freq = 0
        self.overload_last_dt = ''
        self.magnet_dt = ''
        self.magnet_num = 0
        self.magnet_dur = ''
        self.max_dem = ''
        self.max_dem_dt = ''
        self.meter_date = ''
        self.meter_time = ''
        self.power_fail_dt = ''
        self.power_return_dt = ''
        self.power_fail_num = 0
        self.terminal_dt = ''
        self.terminal_num = 0
        self.battery = ''
        self.checksum = ''

# def checkMeterExcel(fileName):
#     excelFile = openpyxl.load_workbook(fileName)
#     sheet = excelFile.active
#     header = {'name': 'نام', 'serial_number': 'شماره سریال', 'rs_dev_addr': 'RS485', 'ramz_rayaneh': 'رمز رایانه', 'shenase_ghabz': 'شناسه قبض', 'shomare_parvande': 'شماره پرونده', 'customer_id': 'نوع مشترک', 'omur': 'امور', 'address': 'آدرس'}
#     maxlen = {'name': 30, 'serial_number': 14, 'rs_dev_addr': 14, 'ramz_rayaneh': 16, 'shenase_ghabz': 16, 'shomare_parvande': 16, 'omur': 20, 'address': 60}
#     customerTypes = db.session.query(Customer.id, Customer.name).all()
#
#     for i, cl in enumerate(sheet.iter_cols()):
#         if i >= len(header):
#             break
#         if cl[0].value.find(list(header.values())[i]) == -1:
#             return 'ERROR: wrong header!'
#
#     df = pd.DataFrame(columns=header.keys())
#     wrongserialnumbers = []
#     for i, row in enumerate(sheet.iter_rows()):
#         # The header row
#         if not i:
#             continue
#
#         rowvals = dict(zip(header.keys(), [None] * len(header)))
#         rowvals['name'] = row[0].value
#
#         if not rowvals['name']:
#             rowvals['name'] = 'کنتور %i' %i
#
#         rowvals['serial_number'] = row[1].value
#
#         if not rowvals['serial_number']:
#             break
#
#         rowvals['serial_number'] = ''.join(re.findall('[0-9]+', str(rowvals['serial_number'])))
#
#         if len(rowvals['serial_number']) > 14 or len(rowvals['serial_number']) < 8:
#             wrongserialnumbers.append(str(row[1].value))
#             continue
#
#         if Meter.query.filter(Meter.serial_number==rowvals['serial_number']).first():
#             wrongserialnumbers.append(str(row[1].value))
#             continue
#
#         rowvals['rs_dev_addr'] = row[2].value
#         if not rowvals['rs_dev_addr']:
#             rowvals['rs_dev_addr'] = rowvals['serial_number']
#         else:
#             rowvals['rs_dev_addr'] = ''.join(re.findall('[0-9]+', str(rowvals['rs_dev_addr'])))
#
#         if len(rowvals['rs_dev_addr']) > 14 or len(rowvals['rs_dev_addr']) < 8:
#             wrongserialnumbers.append(str(row[1].value))
#             continue
#
#         rowvals['ramz_rayaneh'] = row[3].value
#         if rowvals['ramz_rayaneh']:
#             rowvals['ramz_rayaneh'] = ''.join(re.findall('[0-9]+', str(rowvals['ramz_rayaneh'])))
#
#         rowvals['shenase_ghabz'] = row[4].value
#         if rowvals['shenase_ghabz']:
#             rowvals['shenase_ghabz'] = ''.join(re.findall('[0-9]+', str(rowvals['shenase_ghabz'])))
#
#         rowvals['shomare_parvande'] = row[5].value
#         if rowvals['shomare_parvande']:
#             rowvals['shomare_parvande'] = ''.join(re.findall('[0-9]+', str(rowvals['shomare_parvande'])))
#
#         rowvals['customer_id'] = row[6].value
#
#         if not rowvals['customer_id']:
#             rowvals['customer_id'] = 'نامشخص'
#
#         for tempType in customerTypes:
#             if tempType[1].find(rowvals['customer_id']) != -1:
#                 rowvals['customer_id'] = tempType[0]
#                 break
#
#
#         if type(rowvals['customer_id']) != int:
#             wrongserialnumbers.append(str(row[1].value))
#             continue
#
#         rowvals['omur'] = str(row[7].value)
#         rowvals['address'] = str(row[8].value)
#
#         for key in maxlen.keys():
#             if not rowvals[key]:
#                 continue
#             if len(rowvals[key]) > maxlen[key]:
#                 wrongserialnumbers.append(str(row[1].value))
#                 continue
#
#         df = df.append(rowvals, ignore_index=True)
#
#     if wrongserialnumbers == []:
#         return df
#     else:
#         return wrongserialnumbers

db.create_all()
