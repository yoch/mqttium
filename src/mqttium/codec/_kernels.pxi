# Typed, bounded wire operations. Protocol validation remains in Python sources.
from cpython.bytearray cimport PyByteArray_AS_STRING
from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8
from libc.string cimport memcpy


cdef bint _all_continuations(bytearray buffer, Py_ssize_t start, Py_ssize_t available):
    cdef Py_ssize_t i
    for i in range(1, available):
        if not buffer[start + i] & 128:
            return False
    return True


cdef bytes _copy_owned(bytearray buffer, Py_ssize_t start, Py_ssize_t size):
    if start < 0 or size < 0 or start > len(buffer) or size > len(buffer) - start:
        raise IndexError("Frame copy outside decoder storage")
    return PyBytes_FromStringAndSize(PyByteArray_AS_STRING(buffer) + start, size)


cdef tuple _read_u16(bytes buffer, Py_ssize_t offset):
    if len(buffer) - offset < 2:
        raise MalformedPacketError("Incomplete uint16")
    cdef const unsigned char* data = <const unsigned char*>PyBytes_AS_STRING(buffer)
    return (data[offset] << 8) | data[offset + 1], offset + 2


cdef tuple _read_u32(bytes buffer, Py_ssize_t offset):
    if len(buffer) - offset < 4:
        raise MalformedPacketError("Incomplete uint32")
    cdef const unsigned char* data = <const unsigned char*>PyBytes_AS_STRING(buffer)
    cdef unsigned int value = (<unsigned int>data[offset] << 24) | (<unsigned int>data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3]
    return value, offset + 4


cdef tuple _read_utf8(bytes buffer, Py_ssize_t offset):
    if len(buffer) - offset < 2:
        raise MalformedPacketError("Incomplete uint16")
    cdef const unsigned char* data = <const unsigned char*>PyBytes_AS_STRING(buffer)
    cdef Py_ssize_t size = (data[offset] << 8) | data[offset + 1]
    cdef Py_ssize_t start = offset + 2
    if size > len(buffer) - start:
        raise MalformedPacketError("Incomplete UTF-8 string")
    try:
        text = PyUnicode_DecodeUTF8(<const char*>data + start, size, "strict")
    except UnicodeDecodeError as exc:
        raise MalformedPacketError("Invalid UTF-8 data") from exc
    _validate_mqtt_utf8(text)
    return text, start + size


cdef object _assemble_publish(bytes topic, bytes payload, bytes properties,
                              int qos, int flags, unsigned int mid,
                              unsigned int remaining):
    cdef Py_ssize_t topic_size = len(topic)
    cdef Py_ssize_t property_size = len(properties)
    cdef Py_ssize_t payload_size = len(payload)
    cdef Py_ssize_t vbi_size = 1 if remaining < 128 else 2 if remaining < 16384 else 3 if remaining < 2097152 else 4
    cdef Py_ssize_t header_size = 1 + vbi_size + 2 + topic_size + (2 if qos else 0) + property_size
    cdef bint segmented = payload_size >= SEGMENT_THRESHOLD
    cdef bytes result = PyBytes_FromStringAndSize(NULL, header_size + (0 if segmented else payload_size))
    cdef unsigned char* data = <unsigned char*>PyBytes_AS_STRING(result)
    cdef Py_ssize_t pos = 1
    cdef unsigned int value = remaining
    cdef unsigned char digit
    data[0] = 0x30 | flags
    while True:
        digit = value & 0x7f
        value >>= 7
        data[pos] = digit | (0x80 if value else 0)
        pos += 1
        if not value:
            break
    data[pos] = (topic_size >> 8) & 0xff
    data[pos + 1] = topic_size & 0xff
    pos += 2
    memcpy(data + pos, PyBytes_AS_STRING(topic), topic_size)
    pos += topic_size
    if qos:
        data[pos] = (mid >> 8) & 0xff
        data[pos + 1] = mid & 0xff
        pos += 2
    memcpy(data + pos, PyBytes_AS_STRING(properties), property_size)
    pos += property_size
    if segmented:
        return result, payload
    memcpy(data + pos, PyBytes_AS_STRING(payload), payload_size)
    return result
