import sys
import time
import types
import threading

import PyTango
import taurus
from taurus.core.util.codecs import CodecFactory

from Producer import Producer, ProducerClass

VALUE = 9.999


class GroupThread(threading.Thread):

    def __init__(self, dev, strategy):
        super(GroupThread,self).__init__()
        self.dev = dev
        self.codec_obj = self.dev.codec_obj
        if strategy == "event":
            self.strategy = types.MethodType(GroupThread.event,
                                             self,
                                             GroupThread)
        if strategy == "read":
            self.strategy = types.MethodType(GroupThread.read,
                                             self,
                                             GroupThread)
        if strategy == "pipe":
            self.strategy = types.MethodType(GroupThread.read,
                                             self,
                                             GroupThread)

    def strategy(self):
        raise Exception("strategy is not known")

    def event(self, index, data):
        chunk = {}
        for i, d in zip(index, data):
            chunk[str(i)] = d
        format_, chunk_encoded = self.codec_obj.encode(("", chunk))
        for producer in self.dev.producers:
            producer.push_change_event("data", format_, chunk_encoded)

    def read(self, index, data):
        for producer in self.dev.producers:
            producer.append_data(index, data)

    def pipe(self, index, data):
        # PyTango 9 does not support pipe events yet. 
        # For the moment just use the same strategy as read.
        # The consumer will read the pipe.
        for producer in self.dev.producers:
            producer.append_data(index, data)
#         chunk = dict(index=index, data=data)
#         pipe = ("chunk", chunk)
#         for producer in self.dev.producers:
#             producer.push_pipe_event("data_pipe", pipe)

    def run(self):
        producers = self.dev.producers
        no_chunks = self.dev.no_chunks
        points_per_chunk = self.dev.points_per_chunk
        data = [VALUE] * points_per_chunk
        sleep_time = self.dev.sleep_time
        index = 0
        for producer in producers:
            producer.set_state(PyTango.DevState.MOVING)
            producer.push_change_event("state", PyTango.DevState.MOVING)
        for _ in xrange(no_chunks):
            time.sleep(sleep_time)
            if self.dev._stop_flag:
                break
            new_index = index + points_per_chunk
            self.strategy(range(index, new_index), data)
            index = new_index
        for producer in producers:
            producer.set_state(PyTango.DevState.ON)
            producer.push_change_event("state", PyTango.DevState.ON)
        self.dev.set_state(PyTango.DevState.ON)
        self.dev.push_change_event("state", PyTango.DevState.ON)



class GroupClass(PyTango.DeviceClass):

    cmd_list = { 'Start' : [ [ PyTango.ArgType.DevVoid, "r" ],
                             [ PyTango.ArgType.DevVoid, "" ] ],
                 'Stop' : [ [ PyTango.ArgType.DevVoid, "" ],
                                     [ PyTango.ArgType.DevVoid, ""] ],
    }

    attr_list = {'no_chunks' : [ [ PyTango.ArgType.DevLong ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                 'points_per_chunk' : [ [ PyTango.ArgType.DevLong ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                 'sleep_time' : [ [ PyTango.ArgType.DevDouble ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                 'time_per_event' : [ [ PyTango.ArgType.DevDouble ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                 'codec' : [ [ PyTango.ArgType.DevString ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                 'strategy' : [ [ PyTango.ArgType.DevString ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
    }

    def __init__(self, name):
        PyTango.DeviceClass.__init__(self, name)
        self.set_type("TestDevice")


class Group(PyTango.Device_5Impl):

    #@PyTango.DebugIt()
    def __init__(self,cl,name):
        PyTango.Device_5Impl.__init__(self, cl, name)
        self.info_stream('In Group.__init__')
        self.codec = "json"
        self.codec_obj = CodecFactory().getCodec(self.codec)
        self.strategy = "event"
        Group.init_device(self)

    @PyTango.DebugIt()
    def init_device(self):
        self.info_stream('In Python init_device method')
        self.set_change_event('state', True, False)
        self.set_state(PyTango.DevState.ON)
        self.push_change_event("state", PyTango.DevState.ON)
        self.producers = []
        self.no_chunks = 1
        self.points_per_chunk = 1
        self.sleep_time = 0.1
        self.time_per_event = 0
        self._thread = None
        self._stop_flag = False


    #------------------------------------------------------------------

    @PyTango.DebugIt()
    def delete_device(self):
        self.info_stream('PyDsExp.delete_device')

    #------------------------------------------------------------------
    # COMMANDS
    #------------------------------------------------------------------

    @PyTango.DebugIt()
    def is_Start_allowed(self):
        return self.get_state() == PyTango.DevState.ON

    @PyTango.DebugIt()
    def Start(self):
        self.set_state(PyTango.DevState.MOVING)
        self.push_change_event("state", PyTango.DevState.MOVING)
        if self._thread and self._thread.isAlive():
            PyTango.Except.throw_exception('Busy', 
                                           'The previous command execution is still running', 
                                           'Start')
        else:
            self._stop_flag = False
            self.debug_stream('Starting thread...')
            self._thread = GroupThread(self, self.strategy)
            self._thread.setDaemon(True)
            self._thread.start()
        return 

    #------------------------------------------------------------------
    @PyTango.DebugIt()
    def is_Stop_allowed(self):
        return self.get_state() == PyTango.DevState.MOVING

    @PyTango.DebugIt()
    def Stop(self):
        self._stop_flag = True
        return

    #------------------------------------------------------------------
    # ATTRIBUTES
    #------------------------------------------------------------------

    @PyTango.DebugIt()
    def read_attr_hardware(self, data):
        self.info_stream('In read_attr_hardware')

    @PyTango.DebugIt()
    def read_no_chunks(self, attr):
        attr.set_value(self.no_chunks)

    @PyTango.DebugIt()
    def write_no_chunks(self, attr):
        self.no_chunks = attr.get_write_value()

    @PyTango.DebugIt()
    def is_no_chunks_allowed(self, req_type):
        return True

    @PyTango.DebugIt()
    def read_points_per_chunk(self, attr):
        attr.set_value(self.points_per_chunk)

    @PyTango.DebugIt()
    def write_points_per_chunk(self, attr):
        self.points_per_chunk = attr.get_write_value()

    @PyTango.DebugIt()
    def is_points_per_chunk_allowed(self, req_type):
        return True

    @PyTango.DebugIt()
    def read_sleep_time(self, attr):
        attr.set_value(self.sleep_time)

    @PyTango.DebugIt()
    def write_sleep_time(self, attr):
        self.sleep_time = attr.get_write_value()

    @PyTango.DebugIt()
    def is_sleep_time_allowed(self, req_type):
        return True

    @PyTango.DebugIt()
    def read_time_per_event(self, attr):
        attr.set_value(self.time_per_event)

    @PyTango.DebugIt()
    def write_time_per_event(self, attr):
        self.time_per_event = attr.get_write_value()

    @PyTango.DebugIt()
    def is_time_per_event_allowed(self, req_type):
        return True

    @PyTango.DebugIt()
    def read_codec(self, attr):
        attr.set_value(self.codec)

    @PyTango.DebugIt()
    def write_codec(self, attr):
        self.codec = attr.get_write_value()
        self.codec_obj = CodecFactory().getCodec(self.codec)

    @PyTango.DebugIt()
    def is_codec_allowed(self, req_type):
        return True

    @PyTango.DebugIt()
    def read_strategy(self, attr):
        attr.set_value(self.strategy)

    @PyTango.DebugIt()
    def write_strategy(self, attr):
        self.strategy = attr.get_write_value()

    @PyTango.DebugIt()
    def is_strategy_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)


if __name__ == '__main__':
    util = PyTango.Util(sys.argv)
    util.add_class(GroupClass, Group)
    util.add_class(ProducerClass, Producer)

    U = PyTango.Util.instance()
    U.server_init()
    U.server_run()

