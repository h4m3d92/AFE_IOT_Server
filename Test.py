from crc import *

a = '7E 00 63 20 04 00 5C 27 A3 00 01 00 00 00 00 00 00 00 00 80 00 15 03 04 14 02 26 00 00 00 00 00 00 00 00 80 01 15 03 04 02 38 01 00 00 00 00 00 00 31 32 80 04 00 00 00 00 00 00 00 00 00 32 33 30 2E 31 80 08 00 00 00 00 00 00 00 00 31 35 2E 38 32 35 80 0C 00 00 00 00 00 00 00 33 34 35 30 2E 32 35 60 BA 7E'
a = bytearray.fromhex(a)
print(calCrc(a[3:]))

def checkFormat(inStr):
    # inStr = inStr.encode()
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


print(checkFormat(a))
print(int.from_bytes(b'\x002', "big"))
