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
metadata = MetaData()

GroupProject = Table('a_group_project', db.Model.metadata,
                     Column('group_id', SmallInteger, ForeignKey('group.id')),
                     Column('project_id', SmallInteger, ForeignKey('project.id'))
                     )

TaskProject = Table('a_task_project', db.Model.metadata,
                    Column('task_id', SmallInteger, ForeignKey('task.id')),
                    Column('project_id', SmallInteger, ForeignKey('project.id'))
                    )

RolePermission = Table('a_role_permission', db.Model.metadata,
                       Column('role_id', SmallInteger, ForeignKey('role.id')),
                       Column('permission_id', SmallInteger, ForeignKey('permission.id'))
                       )

# Enum Classes
# class RSEnum(enum.Enum):
#     zero = 0
#     one  = 1
#     two  = 2

# User related Classes
class User(db.Model, UserMixin):
    __tablename__ = 'user'

    id            = Column(SmallInteger, primary_key=True)
    username      = Column(String(15), nullable=False, unique=True)
    password      = Column(LargeBinary)
    first_name    = Column(String(15))
    last_name     = Column(String(15))
    is_active     = Column(Boolean, unique=False, default=True)
    mobile        = Column(String(11))
    email         = Column(String(30))
    date_created  = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    group_id      = Column(SmallInteger, ForeignKey('group.id'), nullable=False)
    role_id       = Column(SmallInteger, ForeignKey('role.id'), nullable=False)
    section_id    = Column(SmallInteger, ForeignKey('section.id'))

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

    id            = Column(SmallInteger, primary_key=True)
    name          = Column(String(30), nullable=False, unique=True)
    date_created  = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    project_rel   = relationship("Project", secondary=GroupProject, backref='group') #backref('group', cascade="all,delete")

class Section(db.Model, UserMixin):
    __tablename__ = 'section'

    id            = Column(SmallInteger, primary_key=True)
    name          = Column(String(30), nullable=False)
    group_id      = Column(SmallInteger, ForeignKey('group.id'), nullable=False)
    date_created  = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())

class Permission(db.Model, UserMixin):
    __tablename__ = 'permission'

    id              = Column(SmallInteger, primary_key=True)
    content_id      = Column(SmallInteger, ForeignKey('content.id'))
    p_view          = Column(Boolean, unique=False, default=False)
    p_edit          = Column(Boolean, unique=False, default=False)
    p_add           = Column(Boolean, unique=False, default=False)
    p_delete        = Column(Boolean, unique=False, default=False)
    access_right_id = Column(SmallInteger, ForeignKey('access_right.id'))
    role_rel        = relationship("Role", secondary=RolePermission, backref='permission')


class AccessRight(db.Model, UserMixin):
    __tablename__ = 'access_right'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)


class Content(db.Model, UserMixin):
    __tablename__ = 'content'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String, nullable=False, unique=True)


class Role(db.Model, UserMixin):
    __tablename__ = 'role'

    # metadata=metadata
    id             = Column(SmallInteger, primary_key=True)
    name           = Column(String(15), nullable=False, unique=True)
    permission_rel = relationship("Permission", secondary=RolePermission, backref='role')


# Project Classes
class Project(db.Model, UserMixin):
    __tablename__  = 'project'

    id              = Column(SmallInteger, primary_key=True)
    name            = Column(String(30), nullable=False, unique=True)
    last_comm_time  = Column(DateTime, nullable=True)
    is_edited       = Column(Boolean, unique=False, default=False)
    address         = Column(String, nullable=True, unique=False)
    # coordinates     = Column(Geometry('POINT'), nullable=True)    #sudo apt install postgis postgresql-12-postgis-3     and then     psql mydatabasename -c "CREATE EXTENSION postgis";      on the server terminal
    section_id      = Column(SmallInteger, ForeignKey('section.id')) ######
    date_created    = Column(DateTime(timezone=False), server_default=func.now())
    date_modified   = Column(DateTime(timezone=False), server_default=func.now())
    group_rel       = relationship("Group", secondary=GroupProject, backref='Project')


class ProjectLog(db.Model, UserMixin):
    __tablename__ = 'Project_log'

    id            = Column(Integer, primary_key=True)
    event_type_id = Column(SmallInteger, ForeignKey('event_type.id'), nullable=False)
    project_id    = Column(SmallInteger, ForeignKey('project.id', ondelete='CASCADE'), nullable=False)
    ts            = Column(DateTime(timezone=False), server_default=func.now())


# Task and Event
class Task(db.Model, UserMixin):
    __tablename__ = 'task'

    id            = Column(SmallInteger, primary_key=True)
    name          = Column(String(30), nullable=False, unique=True)
    type_id       = Column(SmallInteger, ForeignKey('task_type.id'), nullable=False)
    execute_time  = Column(DateTime, nullable=False)
    date_created  = Column(DateTime(timezone=False), server_default=func.now())
    date_modified = Column(DateTime(timezone=False), server_default=func.now())
    Project_rel   = relationship("Project", secondary=TaskProject, backref='task')


class TaskType(db.Model, UserMixin):
    __tablename__ = 'task_type'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)


class EventType(db.Model, UserMixin):
    __tablename__ = 'event_type'

    id      = Column(SmallInteger, primary_key=True)
    name    = Column(String(30), nullable=False, unique=True)
    name_fa = Column(String(30), nullable=True, unique=True)


class EventLog(db.Model, UserMixin):
    __tablename__ = 'event_log'

    id            = Column(Integer, primary_key=True)
    timestamp     = Column(DateTime(timezone=False), server_default=func.now())
    event_type_id = Column(SmallInteger, ForeignKey('event_type.id'), nullable=False)
    user_id       = Column(SmallInteger, ForeignKey('user.id'), nullable=False)


# Device and Readout
class Device(db.Model, UserMixin):
    __tablename__ = 'device'

    id               = Column(SmallInteger, primary_key=True)
    name             = Column(String(30), nullable=False)
    serial_number    = Column(String(14), nullable=False)
    address          = Column(String(60), nullable=True)
    e_key            = Column(String(32), nullable=True, unique=False)
    password         = Column(String(16), nullable=False, unique=False)
    is_active        = Column(Boolean, unique=False, default=True)
    is_afe           = Column(Boolean, unique=False, default=True)
    is_master        = Column(Boolean, unique=False, default=False)
    last_comm_time   = Column(DateTime, nullable=True)
    date_created     = Column(DateTime(timezone=False), server_default=func.now())
    date_modified    = Column(DateTime(timezone=False), server_default=func.now())
    project_id       = Column(SmallInteger, ForeignKey('project.id'))
    customer_id      = Column(SmallInteger, ForeignKey('customer.id'))
    replaced_id      = Column(SmallInteger, ForeignKey('device.id'))


class Customer(db.Model, UserMixin):
    __tablename__ = 'customer'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(30), nullable=False, unique=True)


class Readout(db.Model, UserMixin):
    __tablename__  = 'readout'

    id                = Column(Integer, primary_key=True)
    voltage           = Column(Numeric(6, 3))
    current           = Column(Numeric(6, 3))
    energy            = Column(Numeric(14, 3))
    power             = Column(Numeric(14, 3))
    power_factor      = Column(Numeric(6, 3))
    freq              = Column(Numeric(6, 3))
    device_dt         = Column(DateTime, nullable=True)
    power_fail_dt     = Column(DateTime, nullable=True)
    power_return_dt   = Column(DateTime, nullable=True)
    power_fail_num    = Column(Numeric(4, 0))
    meter_cover_dt    = Column(DateTime, nullable=True)
    meter_cover_num   = Column(Numeric(4, 0))
    server_dt         = Column(DateTime(timezone=False), server_default=func.now())
    device_id         = Column(SmallInteger, ForeignKey('device.id'), nullable=False)
    task_id           = Column(SmallInteger, ForeignKey('task.id'), nullable=False)

class ReadoutLog(db.Model, UserMixin):
    __tablename__ = 'readout_log'

    id              = Column(SmallInteger, primary_key=True)
    server_dt       = Column(DateTime(timezone=False), server_default=func.now())
    type_id         = Column(SmallInteger, ForeignKey('readout_type.id'), nullable=False)
    task_id         = Column(SmallInteger, ForeignKey('task.id'), nullable=False)
    device_id        = Column(SmallInteger, ForeignKey('device.id'), nullable=False)

class ReadoutType(db.Model, UserMixin):
    __tablename__ = 'readout_type'

    id   = Column(SmallInteger, primary_key=True)
    name = Column(String(20), nullable=False, unique=True)

# Statistics
class ProjectStats(db.Model, UserMixin):
    __tablename__ = 'project_stats'

    id                 = Column(Integer, primary_key=True)
    type_id            = Column(SmallInteger, ForeignKey('stats_type.id'), nullable=False)
    project_id         = Column(SmallInteger, ForeignKey('project.id'), nullable=False)
    total_device        = Column(SmallInteger, nullable=False)   # total Nu of devices under gw
    succ_device_last    = Column(SmallInteger, nullable=False)   # total Nu of device that read successfully last time
    succ_device_atleast = Column(SmallInteger, nullable=False)   # total Nu of device that read successfully at least once
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

# @login_manager.user_loader
# def user_loader(id):
#     return User.query.filter_by(id=id).first()
#
# @login_manager.request_loader
# def request_loader(request):
#     username = request.form.get('username')
#     user = User.query.filter_by(username=username).first()
#     return user if user else None

db.create_all()
