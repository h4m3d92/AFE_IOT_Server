import socket
import selectors
import types, re
import pandas as pd
import fileHandler as fh
from Crypto.Cipher import AES
import pgDatabase as pgdb
from databaseModel import *
import requests


class serverClass:
    def __init__(self):
        self.gwOnlineDf = pd.DataFrame(
            columns=['ip', 'port', 'gwNumber', 'state', 'taskState', 'meterIdx', 'meters', 'taskId', 'gwLink', 'cache', 'frameCounter', 'validation', 'second',
                     'buffer', 'failReason'])
        self.temp = 0
        temp = pgdb.checkEventType('gw_online', 'gw_offline')
        if type(temp) is str:
            print(temp)
            input()
            exit()

        if self.gwListUpdate() is str:
            input()
            exit()

        self.readoutCnt = db.session.query(Readout.id).order_by(Readout.id.desc()).first()
        if self.readoutCnt:
            self.readoutCnt = self.readoutCnt[0]
        else:
            self.readoutCnt = 0

        print(self.gwList, '\n')

        self.host = configData['host']['ip']
        # self.host = socket.gethostbyname(socket.gethostname())
        # self.host = '0.0.0.0'
        self.port = configData['host']['port']

        self.encMode = AES.MODE_ECB

        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.bind((self.host, self.port))
        self.lsock.listen()
        print(datetime.now(), "Listening on", (self.host, self.port))
        self.lsock.setblocking(False)
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.lsock, selectors.EVENT_READ, data=None)

        self.gwNumLen = 14
        self.edbNumLen = 5
        self.TIME_GET_GWNUM_MAX = 120

        self.onlineStates = {'IDLE': 0, 'GET_GWNUM': 1, 'IN_TASK': 2, 'ON_DEMAND': 3}
        self.taskStates = {'START': 0, 'IDLE': 1, 'REQ_READOUT': 2, 'GET_READOUT': 3}
        self.onDemandStates = {'GET_REQ': 1}
        self.portList = {'zero': 2, 'zero_applied': 1, 'one': 1, 'two': 2}
        self.time = datetime.now()
        self.counter = 0
        fh.writeEvent(['Regular', 'Server start', ''])

    def gwListUpdate(self):
        tempGws = pgdb.readGateways()
        if type(tempGws) is str:
            print(tempGws)
            return tempGws

        self.gwList = pd.DataFrame(
            columns=['gwId', 'gwNumber', 'data_filter', 'ekey', 'password', 'validation'])

        for idx in range(len(tempGws)):
            self.gwList = self.gwList.append({'gwId': tempGws['id'].loc[idx], 'gwNumber':
                int(tempGws['serial_number'].loc[idx]), 'data_filter': tempGws['data_filter'].loc[idx], 'ekey':
                tempGws['e_key'].loc[idx], 'password': tempGws['password'].loc[idx],
                                              'validation': True}, ignore_index=True)

        for idx in range(len(self.gwOnlineDf)):
            if self.gwOnlineDf['gwNumber'].loc[idx]:
                if self.gwList.loc[self.gwList['gwNumber'] == self.gwOnlineDf['gwNumber'].loc[idx]].empty:
                    self.gwOnlineDf['validation'].loc[idx] = False

        print(datetime.now(), 'Gateway list got updated.')

    def timerRoutine(self):
        if datetime.now().second != self.time.second:
            if datetime.now().minute != self.time.minute:
                # print(datetime.now())
                for itask in db.session.query(Task).all():
                    if itask.execute_time.hour == datetime.now().hour and itask.execute_time.minute == datetime.now().minute:
                    # if itask.execute_time.hour == datetime.now().hour:
                        taskGws = db.session.query(Gateway).join(TaskGateway).filter(TaskGateway.c.task_id == itask.id).all()
                        for gws in taskGws:
                            if not self.gwOnlineDf['state'].loc[self.gwOnlineDf['gwNumber'] == int(gws.serial_number)].empty:
                                idx = self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == int(gws.serial_number)].index[0]
                                if self.gwOnlineDf['state'].loc[idx] != self.onlineStates['IDLE']:
                                    continue
                                self.gwOnlineDf['state'].loc[idx] = self.onlineStates['IN_TASK']
                                self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['START']
                                self.gwOnlineDf['gwLink'].loc[idx] = None
                                self.gwOnlineDf['taskId'].loc[idx] = itask.id
                                gwSerailNum = '%014i' %self.gwOnlineDf['gwNumber'].loc[idx]
                                temp = list(db.session.query(Meter.id, Meter.serial_number, Meter.rs_dev_addr, Meter.rs_port_number).join(Gateway).filter(Gateway.serial_number == gwSerailNum).order_by(Meter.id))
                                for i in range(len(temp)):
                                    temp[i] = list(temp[i])+["no_response"]
                                    temp[i][3] = temp[i][3].name

                                self.gwOnlineDf['meters'].loc[idx] = temp
                                self.gwOnlineDf['frameCounter'].loc[idx] = []
                                for cnt, item in enumerate(self.gwOnlineDf['meters'].loc[idx]):
                                    self.gwOnlineDf['frameCounter'].loc[idx] += [bytes([int(cnt/256), cnt%256])]
                                self.gwOnlineDf['meterIdx'].loc[idx] = 0
                                self.gwOnlineDf['second'].loc[idx] = 0
                                print('')
                                print(datetime.now(), 'Readout task started on gateway %s ...' % gwSerailNum)

                if datetime.now().hour != self.time.hour:
                    if datetime.now().hour == 5:
                        self.gwListUpdate()
                        self.writeGatewayStats()

            for idx in range(len(self.gwOnlineDf)):
                self.gwOnlineDf['second'].iloc[idx] += 1
            self.time = datetime.now()


    def gwsRoutine(self):
        for idx in range(len(self.gwOnlineDf)):
            if self.gwOnlineDf['state'].loc[idx] == self.onlineStates['GET_GWNUM'] and self.gwOnlineDf['second'].loc[idx] > self.TIME_GET_GWNUM_MAX:
                self.gwOnlineDf['validation'].loc[idx] = False

            if self.gwOnlineDf['state'].loc[idx] == self.onlineStates['IN_TASK']:
                if self.gwOnlineDf['taskState'].loc[idx] == self.taskStates['START']:
                    if len(self.gwOnlineDf['meters'].loc[idx]):
                        self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['REQ_READOUT']
                        self.gwOnlineDf['second'].loc[idx] = 0
                    else:
                        print(datetime.now(), 'Readout task is finished - gateway: %s!' % self.gwOnlineDf['gwNumber'].loc[idx])
                        self.writeReadoutLog(idx)
                        self.gwOnlineDf['state'].loc[idx] = self.onlineStates['IDLE']

                elif self.gwOnlineDf['taskState'].loc[idx] == self.taskStates['IDLE']:
                    if self.gwOnlineDf['meters'].loc[idx][self.gwOnlineDf['meterIdx'].loc[idx]][3] != 'zero_applied':
                        self.gwOnlineDf['meterIdx'].loc[idx] += 1
                    if self.gwOnlineDf['meterIdx'].loc[idx] < len(self.gwOnlineDf['meters'].loc[idx]):
                        self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['REQ_READOUT']
                        self.gwOnlineDf['second'].loc[idx] = 0
                    else:
                        print(datetime.now(), 'Readout task is finished - gateway: %s!' % self.gwOnlineDf['gwNumber'].loc[idx])
                        self.writeReadoutLog(idx)
                        self.gwOnlineDf['state'].loc[idx] = self.onlineStates['IDLE']

                elif self.gwOnlineDf['taskState'].loc[idx] == self.taskStates['REQ_READOUT']:
                    if self.gwOnlineDf['second'].loc[idx] > 1:
                        self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['GET_READOUT']
                        self.gwOnlineDf['second'].loc[idx] = 0
                        self.sendRoReq(idx, self.gwOnlineDf['meters'].loc[idx][self.gwOnlineDf['meterIdx'].loc[idx]][2])

                if self.gwOnlineDf['second'].loc[idx] > 40:
                    print(datetime.now(), 'No readout - gateway: %i, meter: %s!' % (self.gwOnlineDf['gwNumber'].loc[idx], self.gwOnlineDf['meters'].loc[idx][self.gwOnlineDf['meterIdx'].loc[idx]][2]))
                    self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['IDLE']
                    self.gwOnlineDf['second'].loc[idx] = 0


    def sendRoReq(self, gwIdx, serialNum):
        print(datetime.now(), 'Readout request - gateway: %i, meter: %s' % (self.gwOnlineDf['gwNumber'].loc[gwIdx], serialNum))
        try:
            if self.gwOnlineDf['meters'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]][3] == 'zero':
                self.gwOnlineDf['meters'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]][3] = 'zero_applied'
            elif self.gwOnlineDf['meters'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]][3] == 'zero_applied':
                self.gwOnlineDf['meters'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]][3] = 'zero'
            port = self.gwOnlineDf['meters'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]][3]
            port = self.portList[port]

        except:
            port = 1

        if self.gwList['data_filter'].loc[self.gwList['gwNumber'] == self.gwOnlineDf['gwNumber'].loc[gwIdx]].item():
            filterState = 1
        else:
            filterState = 0
        initBd = 20
        sendStr = b'\x7E\x00\x23\x00\x01'
        sendStr += bytes([0, 12 + len(serialNum)])
        sendStr += self.gwOnlineDf['frameCounter'].loc[gwIdx][self.gwOnlineDf['meterIdx'].loc[gwIdx]]
        sendStr += bytes([port, len(serialNum), filterState, initBd])
        waitTime = 1200
        sendStr += bytes([int(waitTime / 256), waitTime % 256])
        sendStr += bytes(4)
        sendStr += bytes(serialNum, 'utf-8')
        sendStr += bytes(16 - len(serialNum))
        print(datetime.now(), 'Request readout:', sendStr)
        if True:
            encryptor = AES.new(self.gwList['ekey'].loc[self.gwList['gwNumber'] == self.gwOnlineDf['gwNumber'].loc[gwIdx]].item().encode('utf-8'), self.encMode)
            sendStr = sendStr[:3] + encryptor.encrypt(sendStr[3:])
        sendStr += self.calCrc(sendStr[3:], 32)
        sendStr += b'\x7E'
        self.gwOnlineDf['buffer'].loc[gwIdx] = sendStr

    def serverRoutine(self):
        self.timerRoutine()
        events = self.sel.select(timeout=0.001)
        # events = self.sel.select(timeout=10)
        for key, mask in events:
            self.timerRoutine()
            if key.data is None:
                self.accept_wrapper(key)
            else:
                self.service_connection(key, mask)

    def writeReadoutLog(self, gwIdx):
        if self.gwOnlineDf['state'].loc[gwIdx] != self.onlineStates['IN_TASK']:
            return

        if self.gwOnlineDf['gwLink'].loc[gwIdx]:
            if not self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[gwIdx]].empty:
                if self.gwOnlineDf['meters'].loc[gwIdx][0][4] != 'success':
                    # reza=json.dumps({'readout':[{'meter_id': 0, 'readout_id': 0, 'status': 'reza'}]})
                    # self.gwOnlineDf['buffer'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[gwIdx]] = bytes(reza)
                    self.gwOnlineDf['buffer'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[gwIdx]] = self.createHtml(self.gwOnlineDf['meters'].loc[gwIdx][0][0], 0, self.gwOnlineDf['meters'].loc[gwIdx][0][4], self.gwOnlineDf['cache'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[gwIdx]].item())
                    # self.gwOnlineDf['buffer'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[gwIdx]] = b"{'readout':[{'meter_id': %i, 'readout_id': %i, 'status': '%b'}]}" % (self.gwOnlineDf['meters'].loc[gwIdx][0][0], 0, self.gwOnlineDf['meters'].loc[gwIdx][0][4].encode('utf-8'))
        try:
            maxId = db.session.query(ReadoutLog.id).order_by(ReadoutLog.id.desc()).first()
            if maxId:
                maxId = maxId[0] + 1
            else:
                maxId = 1
            for i in range(self.gwOnlineDf['meterIdx'].loc[gwIdx]):
                db.session.add(ReadoutLog(id=maxId,meter_id=self.gwOnlineDf['meters'].loc[gwIdx][i][0],task_id=self.gwOnlineDf['taskId'].loc[gwIdx],type_id=db.session.query(ReadoutType.id).filter(ReadoutType.name == self.gwOnlineDf['meters'].loc[gwIdx][i][4]).first()[0]))
                maxId += 1
            db.session.commit()
            db.session.close()
        except:
            print(datetime.now(), 'ERROR database: cannot write the readout log - gateway: %i!' % self.gwOnlineDf['gwNumber'].loc[gwIdx])
            db.session.close()

    def writeReadout(self, orgRcvReadout):
        # try:
        rcvReadout = str(orgRcvReadout).replace('\x5c\x5c', '\x5c')
        # if it is tap gateway, check the frame counter in order to find correct serial number
        if orgRcvReadout[7:9] not in self.gwOnlineDf['frameCounter'].loc[self.gwIdx]:
            print('ERROR readout: Wrong frame counter!')
            return

        meterIdx = self.gwOnlineDf['frameCounter'].loc[self.gwIdx].index(orgRcvReadout[7:9])
        dbMeterId = self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][0]

        errorMsg = None
        if rcvReadout.find('ERROR') != -1:
            idx = rcvReadout.find('ERROR') + 6
            if rcvReadout[idx] == 'r':
                idx += 3

            if rcvReadout[idx] == '0':
                errorMsg = 'identification timeout'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'ident_timeout'
            elif rcvReadout[idx] == '1':
                errorMsg = 'identification syntax'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'ident_syntax'
            elif rcvReadout[idx] == '2':
                errorMsg = 'identification parity'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'ident_parity'
            elif rcvReadout[idx] == '3':
                errorMsg = 'readout timeout'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'readout_timeout'
            elif rcvReadout[idx] == '4':
                errorMsg = 'readout syntax'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'readout_syntax'
            elif rcvReadout[idx] == '5':
                errorMsg = 'readout parity'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'readout_parity'
            elif rcvReadout[idx] == '6':
                errorMsg = 'readout bcc'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'readout_bcc'
            elif rcvReadout[idx] == '7':
                errorMsg = 'internal'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'internal'
            elif rcvReadout[idx] == '8':
                errorMsg = 'invalid request'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'invalid_request'
            elif rcvReadout[idx] == 'a':
                errorMsg = 'readout overflow'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'readout_overflow'
            else:
                errorMsg = 'unknown error'
                self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'error_unknown'

        if not errorMsg and rcvReadout.find('READOUT') == -1:
            errorMsg = 'bad structure'
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'bad_structure'

        if errorMsg:
            print(datetime.now(), 'ERROR readout: %s - Gateway %i, meter %s!' % (
            errorMsg, self.gwOnlineDf['gwNumber'].loc[self.gwIdx],
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][1]))
            return
        else:
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'success'

        # Check if the identification is on database
        identMsg = re.findall('IDMSG\((.*?)\)', rcvReadout)[0]
        obises = db.session.query(ReadoutMap).filter(ReadoutMap.meter_ident == identMsg).first()
        if not obises:
            print('ERROR readout: Unknown identification message!')
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'ident_unknown'
            return

        temp = re.findall(obises.meter_serial_number_1 + '\((.*?)\)', rcvReadout)
        if temp == []:
            print('ERROR readout: No serial number!')
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'no_serial'
            return
        temp = temp[0]

        if obises.meter_serial_number_2:
            if len(obises.meter_serial_number_2) > 5:
                temp2 = re.findall(obises.meter_serial_number_2 + '\((.*?)\)', rcvReadout)
                if temp2 == []:
                    print('ERROR readout: No serial number 2!')
                    self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'wrong_serial_2'
                    return

                temp = temp2[0] + temp

        meterSerial = int(temp)
        if meterSerial != int(self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][1]):
            print(datetime.now(), 'ERROR readout: Serial number does not match! Expected:', self.gwOnlineDf['meters'].
                  loc[self.gwIdx][self.gwOnlineDf['meterIdx'].loc[self.gwIdx]][1], ', Received:', temp)
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'wrong_serial'
            return
        roParams = readoutParameters()
        self.readoutCnt += 1

        tempDbQuery = "db.session.add(Readout(id=self.readoutCnt,readout_map_id=obises.id,meter_id=dbMeterId,task_id=self.gwOnlineDf['taskId'].loc[self.gwIdx]"
        for par in roParams.__dict__.keys():
            if par in ['meter_ident', 'meter_serial_number_1', 'meter_serial_number_2']:
                continue

            if not obises.__dict__[par]:
                continue

            if len(obises.__dict__[par]) < 3:
                continue

            temp = re.findall(obises.__dict__[par] + '\((.*?)\)', rcvReadout)
            if not temp or temp == []:
                continue

            temp = temp[0]
            if type(roParams.__dict__[par]) == int:
                temp = re.findall('[0-9.]+', temp)
                if temp == [] or not temp:
                    continue
                roParams.__dict__[par] = str(float(temp[0]))
            else:
                if par == 'meter_date' and len(temp) > 8:
                    temp = temp[:8]
                elif par == 'meter_time' and len(temp) > 6:
                    temp = temp[-6:]
                elif par == 'max_dem':
                    temp2 = re.findall('[0-9.]+', temp)
                    if temp2:
                        if len(temp2) == 2:
                            if temp2[0].find('.') != -1:
                                temp = str(float(temp2[0]))
                            elif temp2[1].find('.') != -1:
                                temp = str(float(temp2[1]))
                elif par == 'max_dem_dt':
                    temp2 = re.findall('[0-9.]+', temp)
                    if temp2:
                        if len(temp2) == 2:
                            if temp2[0].find('.') == -1:
                                temp = temp2[0]
                            elif temp2[1].find('.') == -1:
                                temp = temp2[1]
                roParams.__dict__[par] = "'" + temp + "'"

            tempDbQuery += ',' + par + '=' + roParams.__dict__[par]

        tempDbQuery += '))'

        try:
            exec(tempDbQuery)
            db.session.commit()
            db.session.close()
            if self.gwOnlineDf['gwLink'].loc[self.gwIdx]:
                if not self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[self.gwIdx]].empty:
                    self.gwOnlineDf['buffer'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[self.gwIdx]] = self.createHtml(dbMeterId, self.readoutCnt, 'success', self.gwOnlineDf['cache'].loc[self.gwOnlineDf['gwNumber'] == self.gwOnlineDf['gwLink'].loc[self.gwIdx]].item())

        except:
            print(tempDbQuery)
            print(datetime.now(), 'ERROR database: cannot write the readout data - gateway: %i, meter: %i!' % (self.gwOnlineDf['gwNumber'].loc[self.gwIdx], meterSerial))
            db.session.close()
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][4] = 'error_database'
        if self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][3].find('zero') != -1:
            portNum = self.portList[self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][3]]
            if portNum == 1:
                portNum = 'one'
            else:
                portNum = 'two'
            self.gwOnlineDf['meters'].loc[self.gwIdx][meterIdx][3] = portNum
            try:
                db.session.query(Meter).filter(Meter.id == int(dbMeterId)).update({'rs_port_number': portNum})
                db.session.commit()
                db.session.close()
            except:
                print(datetime.now(), 'ERROR database: cannot write the meter port!')
                db.session.close()

    def checkIncomeMsg(self, rcvData):
        if self.gwOnlineDf.loc[self.gwIdx].empty:
            return

        try:
            temp = self.checkFormat(rcvData)
            if temp:
                print(datetime.now(), 'ERROR: received message is incorrect - %s!' %temp)
                self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                return

            if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['IDLE']:
                # self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                # self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Unexpected income message'
                return

            if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['ON_DEMAND']:
                rcvData = str(rcvData)
                meterId = re.findall('ondemand\?meter_id=([0-9]+)', rcvData)
                if meterId == []:
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    return
                self.gwOnlineDf['cache'].loc[self.gwIdx] = True
                try:
                    meterId = int(meterId[0])
                    temp = db.session.query(Meter.id, Meter.serial_number, Meter.rs_dev_addr, Meter.rs_port_number, Meter.gateway_id).join(
                            Gateway).filter(Meter.id == meterId).order_by(Meter.id).first()
                    if temp:
                        temp = list(temp)
                    else:
                        self.gwOnlineDf['buffer'].loc[self.gwIdx] = self.createHtml(meterId, 0, 'no_meter', self.gwOnlineDf['cache'].loc[self.gwIdx])
                        return

                    gwNum = int(db.session.query(Gateway.serial_number).filter(Gateway.id == temp[4]).first()[0])
                    if self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == gwNum].empty:
                        # print(self.gwOnlineDf)
                        self.gwOnlineDf['buffer'].loc[
                            self.gwIdx] = self.createHtml(meterId, 0, 'gateway_offline', self.gwOnlineDf['cache'].loc[self.gwIdx])
                        return
                    idx = self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == gwNum].index[0]
                    if self.gwOnlineDf['state'].loc[idx] != self.onlineStates['IDLE']:
                        self.gwOnlineDf['buffer'].loc[self.gwIdx] = self.createHtml(meterId, 0, 'gateway_busy', self.gwOnlineDf['cache'].loc[self.gwIdx])
                        return

                    try:
                        self.gwOnlineDf['taskId'].loc[idx] = db.session.query(Task.id).join(TaskType).filter(TaskType.name == "on_demand").first()[0]
                    except:
                        self.gwOnlineDf['buffer'].loc[self.gwIdx] = self.createHtml(meterId, 0, 'no_task_id', self.gwOnlineDf['cache'].loc[self.gwIdx])
                        return
                    self.gwOnlineDf['state'].loc[idx] = self.onlineStates['IN_TASK']
                    self.gwOnlineDf['taskState'].loc[idx] = self.taskStates['START']
                    self.gwOnlineDf['gwLink'].loc[idx] = self.gwOnlineDf['gwNumber'].loc[self.gwIdx]
                    self.gwOnlineDf['meters'].loc[idx] = [temp[:3] + [temp[3].name, "no_response"]]
                    self.gwOnlineDf['frameCounter'].loc[idx] = []
                    for cnt, item in enumerate(self.gwOnlineDf['meters'].loc[idx]):
                        self.gwOnlineDf['frameCounter'].loc[idx] += [bytes([int(cnt / 256), cnt % 256])]
                    self.gwOnlineDf['meterIdx'].loc[idx] = 0
                    self.gwOnlineDf['second'].loc[idx] = 2
                    print('')
                    print(datetime.now(), 'Readout task started on gateway %s ...' % self.gwOnlineDf['gwNumber'].loc[idx])

                    return
                except:
                    self.gwOnlineDf['buffer'].loc[self.gwIdx] = self.createHtml(meterId, 0, 'something_wrong', self.gwOnlineDf['cache'].loc[self.gwIdx])
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                return

            if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['IN_TASK']:
                if self.gwOnlineDf['meterIdx'].loc[self.gwIdx] >= len(self.gwOnlineDf['meters'].loc[self.gwIdx]):
                    print(datetime.now(), 'ERROR task: It is not readout mode - Gateway %i!' % self.gwOnlineDf['gwNumber'].loc[self.gwIdx])
                    return

                encryptor = AES.new(self.gwList['ekey'].loc[self.gwList['gwNumber'] == self.gwOnlineDf['gwNumber'].loc[self.gwIdx]].item().encode('utf-8'), self.encMode)
                rcvData = rcvData[:3] + encryptor.decrypt(rcvData[3:int((len(rcvData)-6) / 16) * 16 + 3]) + rcvData[int((len(rcvData)-6) / 16) * 16 + 3:]

                print(datetime.now(), 'Readout received - gateway: %i, data:' % self.gwOnlineDf['gwNumber'].loc[self.gwIdx], rcvData)
                if len(rcvData) < 12:
                    return

                self.writeReadout(rcvData)
                self.gwOnlineDf['taskState'].loc[self.gwIdx] = self.taskStates['IDLE']
                return

            if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['GET_GWNUM']:
                if len(rcvData) != 38:
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Unexpected income length'
                    print(datetime.now(), 'ERROR: Unexpected income length!')
                    return

                rcvData = rcvData[21:35]
                try:
                    rcvData = int(rcvData.decode())
                except:
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Incorrect serial number'
                    print(datetime.now(), 'ERROR: Incorrect serial number!11')
                    return

                if len(str(rcvData)) != self.gwNumLen or self.gwList.loc[self.gwList['gwNumber'] == rcvData].empty:
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Wrong gateway number'
                    print(datetime.now(), 'ERROR: Wrong gateway number!')
                    return

                if not self.gwList['validation'].loc[self.gwList['gwNumber'] == rcvData].item():
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Invalid gateway number'
                    return
                if not self.gwOnlineDf.loc[self.gwOnlineDf['gwNumber'] == rcvData].empty:
                    self.gwOnlineDf['validation'].loc[self.gwOnlineDf['gwNumber'] == rcvData] = False
                    self.gwOnlineDf['failReason'].loc[self.gwOnlineDf['gwNumber'] == rcvData] = 'Repetitive gateway number'
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    self.gwOnlineDf['failReason'].loc[self.gwIdx] = 'Repetitive gateway number'
                    return
                # pgdb.updateGwStatus(rcvData, 1)
                self.gwOnlineDf['gwNumber'].loc[self.gwIdx] = rcvData
                pgdb.writeEventLog(self.gwOnlineDf['gwNumber'].loc[self.gwIdx], 'gw_online')
                fh.writeConnection(['Registered', self.gwOnlineDf['ip'].loc[self.gwIdx], self.gwOnlineDf['port'].loc[self.gwIdx],
                                    self.gwOnlineDf['gwNumber'].loc[self.gwIdx], None])
                self.gwOnlineDf['state'].loc[self.gwIdx] = self.onlineStates['IDLE']
                return
        except:
            return

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        if self.gwOnlineDf.loc[(self.gwOnlineDf['ip'] == data.addr[0]) & (self.gwOnlineDf['port'] == data.addr[1])].empty:
            fh.writeEvent(['Error', 'Err_002', 'The socket is not registered!'])
            try:
                self.sel.unregister(sock)
                sock.close()
                return
            except:
                return
        else:
            self.gwIdx = self.gwOnlineDf.loc[(self.gwOnlineDf['ip'] == data.addr[0]) & (self.gwOnlineDf['port'] == data.addr[1])].index[0]
            if not self.gwOnlineDf['validation'].loc[self.gwIdx]:
                try:
                    if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['IN_TASK']:
                        self.writeReadoutLog(self.gwIdx)
                    pgdb.writeEventLog(self.gwOnlineDf['gwNumber'].loc[self.gwIdx], 'gw_offline')
                    # pgdb.updateGwStatus(self.gwOnlineDf['gwNumber'].loc[self.gwIdx], 0)
                    fh.writeConnection(['Disconnected', self.gwOnlineDf['ip'].loc[self.gwIdx], self.gwOnlineDf['port'].loc[self.gwIdx],
                                        self.gwOnlineDf['gwNumber'].loc[self.gwIdx], self.gwOnlineDf['failReason'].loc[self.gwIdx]])
                    self.gwOnlineDf.drop(index=self.gwIdx, inplace=True)
                    self.gwOnlineDf = self.gwOnlineDf.reset_index(drop=True)
                    print(datetime.now(), "Closing connection to", data.addr)
                    self.sel.unregister(sock)
                    sock.close()
                    return
                except:
                    fh.writeEvent(['Error', 'Err_003', 'The socket did not closed properly!'])
                    return

        if mask & selectors.EVENT_READ:
            # try if the connection is alive
            try:
                recv_data = sock.recv(1024)  # Should be ready to read
                if recv_data:
                    fh.writeTransmission(['Received', self.gwOnlineDf['ip'].loc[self.gwIdx], self.gwOnlineDf['port'].loc[self.gwIdx],
                                          self.gwOnlineDf['gwNumber'].loc[self.gwIdx], str(recv_data)[2:-1]])
                    print(datetime.now(), 'Received', recv_data, "from", data.addr)
                    rcvResult = self.checkIncomeMsg(recv_data)

                    if rcvResult:
                        data.outb += rcvResult
                else:
                    self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                    return
            except:
                self.gwOnlineDf['validation'].loc[self.gwIdx] = False
                return

        if mask & selectors.EVENT_WRITE:
            if self.gwOnlineDf['buffer'].loc[self.gwIdx] != b'':
                data.outb = self.gwOnlineDf['buffer'].loc[self.gwIdx]
                self.gwOnlineDf['buffer'].loc[self.gwIdx] = b''

            if data.outb:
                fh.writeTransmission(['Sent', self.gwOnlineDf['ip'].loc[self.gwIdx], self.gwOnlineDf['port'].loc[self.gwIdx],
                                      self.gwOnlineDf['gwNumber'].loc[self.gwIdx], str(data.outb)[2:-1]])
                print(datetime.now(), "Sending", repr(data.outb), "to", data.addr)
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
                # if self.gwOnlineDf['state'].loc[self.gwIdx] == self.onlineStates['ON_DEMAND']:
                #     self.gwOnlineDf['validation'].loc[self.gwIdx] = False


    def accept_wrapper(self, key):
        sock = key.fileobj
        conn, addr = sock.accept()  # Should be ready to read
        print(datetime.now(), "Accepted connection from", addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

        # if this IP port is not registered:
        if self.gwOnlineDf.loc[(self.gwOnlineDf['ip'] == addr[0]) & (self.gwOnlineDf['port'] == addr[1])].empty:
            if addr[0] == configData["front_end"]["ip"] and self.temp:
                self.gwOnlineDf = self.gwOnlineDf.append(
                    {'ip': addr[0], 'port': addr[1], 'state': self.onlineStates['ON_DEMAND'], 'gwNumber': int(addr[1]),
                     'second': 0, 'validation': True, 'buffer': b'', 'failReason': ''}, ignore_index=True)
            else:
                self.temp = 1
                self.gwOnlineDf = self.gwOnlineDf.append(
                    {'ip': addr[0], 'port': addr[1], 'state': self.onlineStates['GET_GWNUM'], 'gwNumber': None,
                     'second': 0, 'validation': True, 'buffer': b'', 'failReason': ''}, ignore_index=True)
                data.outb = b'\x7E\x00\x13\x20\x01\x00\x0C\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xD2\x39\x7E'
        else:
            fh.writeEvent(['Error', 'Err_005', str(addr) + 'The ip and port is repetitive!'])
            self.gwOnlineDf = self.gwOnlineDf.append({'ip': addr[0], 'port': addr[1], 'state': self.onlineStates['GET_GWNUM'], 'gwNumber': None,
                                    'second': 0, 'validation': False, 'buffer': b'', 'failReason': 'ip port'}, ignore_index=True)
            self.gwOnlineDf['validation'].loc[(self.gwOnlineDf['ip'] == addr[0]) & (self.gwOnlineDf['port'] == addr[1])] = False
            self.gwOnlineDf['failReason'].loc[(self.gwOnlineDf['ip'] == addr[0]) & (self.gwOnlineDf['port'] == addr[1])] = 'ip port'

        fh.writeConnection(['Connected', addr[0], addr[1], None, None])

    def writeGatewayStats(self):
        try:
            gws = db.session.query(Gateway.id).all()
            for i in gws:
                gwIdx = i[0]
                try:
                    dashboardTypeId = db.session.query(StatsType.id).filter(StatsType.name == 'dashboard').first()[0]
                except:
                    print(datetime.now(), 'ERROR database: No "dashboard" name in stats_type table!')
                    db.session.close()
                    continue
                meterNum = db.session.query(Meter).filter(Meter.gateway_id == gwIdx).count()
                meterNum
                time_today = datetime.now().date()
                time_yesterday = time_today - timedelta(days=1)
                totalRo = db.session.query(ReadoutLog).join(Meter).filter(Meter.gateway_id == gwIdx).filter(
                    ReadoutLog.server_dt < time_today).filter(ReadoutLog.server_dt > time_yesterday).count()
                try:
                    successTypeId = db.session.query(ReadoutType.id).filter(ReadoutType.name == 'success').first()[0]
                except:
                    print(datetime.now(), 'ERROR database: No "success" name in readout_type table!')
                    db.session.close()
                    continue
                successRo = db.session.query(ReadoutLog).join(Meter).filter(Meter.gateway_id == gwIdx).filter(
                    ReadoutLog.type_id == successTypeId).filter(ReadoutLog.server_dt < time_today).filter(
                    ReadoutLog.server_dt > time_yesterday).count()
                try:
                    identTimeout = db.session.query(ReadoutType.id).filter(ReadoutType.name == 'ident_timeout').first()[0]
                except:
                    print(datetime.now(), 'ERROR database: No "ident_timeout" name in readout_type table!')
                    db.session.close()
                    continue
                identTimeoutRo = db.session.query(ReadoutLog).join(Meter).filter(Meter.gateway_id == gwIdx).filter(
                    ReadoutLog.type_id == identTimeout).filter(ReadoutLog.server_dt < time_today).filter(
                    ReadoutLog.server_dt > time_yesterday).count()
                otherErrorRo = totalRo - successRo - identTimeoutRo
                successAtleast = db.session.query(ReadoutLog).join(Meter).filter(Meter.gateway_id == gwIdx).filter(
                    ReadoutLog.type_id == successTypeId).filter(ReadoutLog.server_dt < time_today).filter(
                    ReadoutLog.server_dt > time_yesterday).distinct(ReadoutLog.meter_id).count()
                maxId = db.session.query(GatewayStats.id).order_by(GatewayStats.id.desc()).first()
                if maxId:
                    maxId = maxId[0] + 1
                else:
                    maxId = 1
                db.session.add(GatewayStats(id=maxId, type_id=dashboardTypeId, gateway_id=gwIdx, total_meter=meterNum,
                                            succ_meter_last=successAtleast, succ_meter_atleast=successAtleast,
                                            total_read_daily=totalRo, succ_read_daily=successRo,
                                            err1_read_daily=identTimeoutRo, err2_read_daily=otherErrorRo,
                                            err3_read_daily=0))
                db.session.commit()
                db.session.close()
        except:
            print(datetime.now(), 'ERROR database: Something is wrong with writing the gateway_stats!')
            db.session.close()

    def checkFormat(self, inStr):
        if len(inStr) < 22:
            return 'Short frame'

        # The first and last flag should be 7E
        if inStr[0] != 126 or inStr[-1] != 126:
            return 'Incorrect flag'

        frameLen = int.from_bytes(inStr[1:3], "big")
        if frameLen != len(inStr) - 3:
            return 'Incorrect frame length'

        if frameLen % 16 != 3:
            return 'Invalid frame length'

        if int.from_bytes(inStr[5:7], "big") > frameLen - 7:
            return 'Incorrect data length'

        if self.calCrc(inStr[3:-3], frameLen - 3) != inStr[-3:-1]:
            return 'Incorrect CRC'

    def createHtml(self, meterNum, roNum, status, doesCache):
        # if doesCache:
        #     doesCache = 2592000
        #     doesCache = 300000
        # else:
        #     doesCache = 5
        doesCache = 5
        tlen = 110 + len(str(int(meterNum)) + str(int(roNum)) + status)
        tData = 'HTTP/1.1 200 OK\r\nDate: ' + datetime.now(tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S") + 'GMT\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json\r\nContent-Length: %i\r\nConnection: keep-alive\r\nCache-Control: max-age=%i, must-revalidate\r\nCF-Cache-Status: HIT\r\nAccept-Ranges: bytes\r\n\r\n{\n   "readouts" : [\n      {\n         "meter_id" : %i,\n         "readout_id" : %i\n      }\n   ],\n   "status" : "%s"\n}\n' % (tlen, doesCache, int(meterNum), int(roNum), status)
        # tData = 'HTTP/1.1 200 OK\r\nDate: ' + datetime.now(tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S") + 'GMT\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json\r\nContent-Length: %i\r\nConnection: keep-alive\r\nCache-Control: max-age=%i, no-transform\r\nCF-Cache-Status: HIT\r\nAge: 557\r\nAccept-Ranges: bytes\r\n\r\n{\n   "readouts" : [\n      {\n         "meter_id" : %i,\n         "readout_id" : %i\n      }\n   ],\n   "status" : "%s"\n}\n' % (tlen, doesCache, int(meterNum), int(roNum), status)

        return tData.encode()

    def calCrc(self, inStr, inLen):
        dnpCrcTable = [
            0x0000, 0x3D65, 0x7ACA, 0x47AF, 0xF594, 0xC8F1, 0x8F5E, 0xB23B, 0xD64D, 0xEB28, 0xAC87, 0x91E2, 0x23D9,
            0x1EBC, 0x5913, 0x6476, 0x91FF, 0xAC9A, 0xEB35, 0xD650, 0x646B, 0x590E, 0x1EA1, 0x23C4, 0x47B2, 0x7AD7,
            0x3D78, 0x001D, 0xB226, 0x8F43, 0xC8EC, 0xF589, 0x1E9B, 0x23FE, 0x6451, 0x5934, 0xEB0F, 0xD66A, 0x91C5,
            0xACA0, 0xC8D6, 0xF5B3, 0xB21C, 0x8F79, 0x3D42, 0x0027, 0x4788, 0x7AED, 0x8F64, 0xB201, 0xF5AE, 0xC8CB,
            0x7AF0, 0x4795, 0x003A, 0x3D5F, 0x5929, 0x644C, 0x23E3, 0x1E86, 0xACBD, 0x91D8, 0xD677, 0xEB12, 0x3D36,
            0x0053, 0x47FC, 0x7A99, 0xC8A2, 0xF5C7, 0xB268, 0x8F0D, 0xEB7B, 0xD61E, 0x91B1, 0xACD4, 0x1EEF, 0x238A,
            0x6425, 0x5940, 0xACC9, 0x91AC, 0xD603, 0xEB66, 0x595D, 0x6438, 0x2397, 0x1EF2, 0x7A84, 0x47E1, 0x004E,
            0x3D2B, 0x8F10, 0xB275, 0xF5DA, 0xC8BF, 0x23AD, 0x1EC8, 0x5967, 0x6402, 0xD639, 0xEB5C, 0xACF3, 0x9196,
            0xF5E0, 0xC885, 0x8F2A, 0xB24F, 0x0074, 0x3D11, 0x7ABE, 0x47DB, 0xB252, 0x8F37, 0xC898, 0xF5FD, 0x47C6,
            0x7AA3, 0x3D0C, 0x0069, 0x641F, 0x597A, 0x1ED5, 0x23B0, 0x918B, 0xACEE, 0xEB41, 0xD624, 0x7A6C, 0x4709,
            0x00A6, 0x3DC3, 0x8FF8, 0xB29D, 0xF532, 0xC857, 0xAC21, 0x9144, 0xD6EB, 0xEB8E, 0x59B5, 0x64D0, 0x237F,
            0x1E1A, 0xEB93, 0xD6F6, 0x9159, 0xAC3C, 0x1E07, 0x2362, 0x64CD, 0x59A8, 0x3DDE, 0x00BB, 0x4714, 0x7A71,
            0xC84A, 0xF52F, 0xB280, 0x8FE5, 0x64F7, 0x5992, 0x1E3D, 0x2358, 0x9163, 0xAC06, 0xEBA9, 0xD6CC, 0xB2BA,
            0x8FDF, 0xC870, 0xF515, 0x472E, 0x7A4B, 0x3DE4, 0x0081, 0xF508, 0xC86D, 0x8FC2, 0xB2A7, 0x009C, 0x3DF9,
            0x7A56, 0x4733, 0x2345, 0x1E20, 0x598F, 0x64EA, 0xD6D1, 0xEBB4, 0xAC1B, 0x917E, 0x475A, 0x7A3F, 0x3D90,
            0x00F5, 0xB2CE, 0x8FAB, 0xC804, 0xF561, 0x9117, 0xAC72, 0xEBDD, 0xD6B8, 0x6483, 0x59E6, 0x1E49, 0x232C,
            0xD6A5, 0xEBC0, 0xAC6F, 0x910A, 0x2331, 0x1E54, 0x59FB, 0x649E, 0x00E8, 0x3D8D, 0x7A22, 0x4747, 0xF57C,
            0xC819, 0x8FB6, 0xB2D3, 0x59C1, 0x64A4, 0x230B, 0x1E6E, 0xAC55, 0x9130, 0xD69F, 0xEBFA, 0x8F8C, 0xB2E9,
            0xF546, 0xC823, 0x7A18, 0x477D, 0x00D2, 0x3DB7, 0xC83E, 0xF55B, 0xB2F4, 0x8F91, 0x3DAA, 0x00CF, 0x4760,
            0x7A05, 0x1E73, 0x2316, 0x64B9, 0x59DC, 0xEBE7, 0xD682, 0x912D, 0xAC48]

        crc = 0
        for i in range(inLen):
            crc = ((crc << 8) ^ dnpCrcTable[((crc >> 8) ^ inStr[i]) & 0x00FF]) & 0xFFFF
        crc ^= 0xFFFF
        return bytes([int(crc / 256) & 0xFF, crc & 0xFF])


server = serverClass()

# try:
while True:
    server.serverRoutine()
    server.gwsRoutine()
# # except:
# #     fh.writeEvent(['Error', 'Err_001', 'Caught keyboard interrupt, Exiting'])
# #     print(datetime.now(), "caught keyboard interrupt, exiting")
# #     resp = requests.post('https://textbelt.com/text', {
# #         'phone': configData["sms"]["phone"],
# #         'message': configData["sms"]["message"],
# #         'key': 'textbelt',
# #     })
# #     print(resp.json())
# finally:
#     for i in range(len(server.gwOnlineDf)):
#         pgdb.writeEventLog(server.gwOnlineDf['gwNumber'].loc[i], 'gw_offline')
#     # db.disconnectGwStatuses()
#     server.sel.close()
#     exit()
