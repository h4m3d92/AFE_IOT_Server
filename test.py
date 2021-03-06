from crc import *
import re


a = b'10409900994217'
print(int(a.decode()))

rcvStr = '7E 00 63 20 04 00 5C 27 A3 00 01 00 00 00 00 00 00 00 00 80 00 15 03 04 14 02 26 00 00 00 00 00 00 00 00 80 F0 10 22 64 02 29 10 12 01 44 FF FF FF 00 00 80 01 15 03 04 02 38 01 00 00 00 00 00 00 31 32 80 04 00 00 00 00 00 00 00 00 00 32 33 30 2E 31 80 08 00 00 00 00 00 00 00 00 31 35 2E 38 32 35 80 0C 00 00 00 00 00 00 00 33 34 35 30 2E 32 35 60 BA 7E'
rcvStr = bytearray.fromhex(rcvStr)
# print(calCrc(rcvStr[3:]))


def checkFormat(inStr):
    if len(inStr) < 22:
        return 'Short frame'

    # The first and last flag should be 7E
    if inStr[0] != 126 or inStr[-1] != 126:
        return 'Incorrect flag'

    frameLen = int.from_bytes(inStr[1:3], "big")
    if frameLen != len(inStr) - 3:
        return 'Incorrect frame length'

    if frameLen%16 != 3:
        return 'Invalid frame length'

    if int.from_bytes(inStr[5:7], "big") > frameLen - 7:
        return 'Incorrect data length'

    if calCrc(inStr[3:-3]) != inStr[-3:-1]:
        return 'Incorrect CRC'

readoutMap = {
    'serial_number'       : b'\x80\x80',
    'slave_dt'            : b'\x80\x00',
    'broken_device_dt'    : b'\x80\x01',
    'broken_device_num'   : b'\x80\x01',
    'device_state'        : b'\x80\x02',
    'voltage'             : b'\x80\x04',
    'voltage_1'           : b'\x80\x05',
    'voltage_2'           : b'\x80\x06',
    'voltage_3'           : b'\x80\x07',
    'current'             : b'\x80\x08',
    'current_1'           : b'\x80\x09',
    'current_2'           : b'\x80\x0A',
    'current_3'           : b'\x80\x0B',
    'power'               : b'\x80\x0C',
    'power_1'             : b'\x80\x0D',
    'power_2'             : b'\x80\x0E',
    'power_3'             : b'\x80\x0F',
    'energy'              : b'\x80\x10',
    'energy_1'            : b'\x80\x11',
    'energy_2'            : b'\x80\x12',
    'energy_3'            : b'\x80\x13',
    'frequency'           : b'\x80\x14',
    'power_fail_dt'       : b'\x80\x15',
    'power_return_dt'     : b'\x80\x16',
    'power_fail_num'      : b'\x80\x17',
    'schedule'            : b'\x80\xF0'
}

readoutBuf = {
    'serial_number'       : 'int',
    'slave_dt'            : 'dt',
    'broken_device_dt'    : 'dt',
    'broken_device_num'   : 'int',
    'device_state'        : 'int',
    'voltage'             : 'float',
    'voltage_1'           : 'float',
    'voltage_2'           : 'float',
    'voltage_3'           : 'float',
    'current'             : 'float',
    'current_1'           : 'float',
    'current_2'           : 'float',
    'current_3'           : 'float',
    'power'               : 'float',
    'power_1'             : 'float',
    'power_2'             : 'float',
    'power_3'             : 'float',
    'energy'              : 'float',
    'energy_1'            : 'float',
    'energy_2'            : 'float',
    'energy_3'            : 'float',
    'frequency'           : 'float',
    'power_fail_dt'       : 'dt',
    'power_return_dt'     : 'dt',
    'power_fail_num'      : 'int',
    'schedule'            : 'byte'
}


frameLen = rcvStr[1:3]
obisNum = int((len(rcvStr)-22)/16)
for i in range(obisNum):
    item = rcvStr[19+i*16: 35+i*16]
    dataCode = None
    for name, obis in readoutMap.items():
        if obis == item[:2]:
            dataCode = name
            print(dataCode)
            if readoutBuf[dataCode] == 'int':
                if name == 'broken_device_num':
                    # The first 6 bytes are date and time
                    try:
                        readoutBuf[dataCode] = int(re.findall('[0-9]+', item[8:].decode())[0])
                    except:
                        readoutBuf[dataCode] = 0
                else:
                    try:
                        readoutBuf[dataCode] = int(re.findall('[0-9]+', item[2:].decode())[0])
                    except:
                        readoutBuf[dataCode] = 0
                print(readoutBuf[dataCode])
            elif readoutBuf[dataCode] == 'float':
                try:
                    readoutBuf[dataCode] = round(float(re.findall('[.0-9]+', item[8:].decode())[0]), 3)
                except:
                    readoutBuf[dataCode] = 0
                print(readoutBuf[dataCode])
            elif readoutBuf[dataCode] == 'dt':
                # Check the format of date and time. maximum year is 2080
                if item[2] > 80 or not item[3]*item[4] or item[3] > 12 or item[4] > 31 or item[5] > 23 or item [6] > 59 or item[7] > 59:
                    continue
                readoutBuf[dataCode] = '%i-%02i-%02i %02i:%02i:%02i' % (2000 + item[2], item[3], item[4], item[5], item[6], item[7])
                print(readoutBuf[dataCode])
            elif readoutBuf[dataCode] == 'byte':
                readoutBuf[dataCode] = b''
                for k in range(4):
                    if item[2 + k * 3] != 255:
                        if item[2+k*3] > 23 or item[3+k*3] > 59 or item[4+k*3] > 100:
                            continue
                        else:
                            readoutBuf[dataCode] += bytes([item[2+k*3], item[3+k*3], item[4+k*3]])
                if readoutBuf[dataCode] == b'':
                    continue
                print(readoutBuf[dataCode])
            # print(checkFormat(rcvStr))
