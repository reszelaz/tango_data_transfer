import sys
import time
import copy
import threading
import numpy
import PyTango

class ProducerClass(PyTango.DeviceClass):

    attr_list = {'data' : [ [ PyTango.ArgType.DevString ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ] ],
    }
    
    pipe_list = {'data_pipe' : [ PyTango.PipeWriteType.PIPE_READ ]
    }

    def __init__(self, name):
        PyTango.DeviceClass.__init__(self, name)
        self.set_type("TestDevice")


class Producer(PyTango.Device_5Impl):

    #@PyTango.DebugIt()
    def __init__(self,cl,name):
        PyTango.Device_5Impl.__init__(self, cl, name)
        self.info_stream('In Producer.__init__')
        Producer.init_device(self)

    @PyTango.DebugIt()
    def init_device(self):
        self.info_stream('In Python init_device method')
        self.set_state(PyTango.DevState.ON)
        self.set_change_event('data', True, False)
        util = PyTango.Util.instance()
        self.group = util.get_device_list_by_class("Group")[0]
        self.group.producers.append(self)
        self.lock = threading.Lock()
        self.index = []
        self.data = []

    #------------------------------------------------------------------

    @PyTango.DebugIt()
    def delete_device(self):
        self.info_stream('PyDsExp.delete_device')

    #------------------------------------------------------------------
    # ATTRIBUTES
    #------------------------------------------------------------------

    @PyTango.DebugIt()
    def read_attr_hardware(self, data):
        self.info_stream('In read_attr_hardware')

    @PyTango.DebugIt()
    def read_data(self, attr):
        with self.lock:
            index = copy.copy(self.index)
            data = copy.copy(self.data)
            self.data, self.index = [], []
        chunk = dict(index=index, data=data)
        _, chunk_encoded = self.group.codec_obj.encode(("", chunk))
        attr.set_value(chunk_encoded)

    @PyTango.DebugIt()
    def is_data_allowed(self, req_type):
        return True #self.get_state() in (PyTango.DevState.ON,)
    
    @PyTango.DebugIt()
    def read_data_pipe(self, pipe):
        with self.lock:
            index = copy.copy(self.index)
            data = copy.copy(self.data)
            self.data, self.index = [], []
        print index, data
        chunk = {}
        for i, d in zip(index, data):
            chunk[str(i)] = d
        pipe.set_value(('name', chunk))
    
    @PyTango.DebugIt()
    def is_data_pipe_allowed(self, req_type):
        return True #self.get_state() in (PyTango.DevState.ON,)

    def append_data(self, index, data):
        with self.lock:
            self.index.append(index)
            self.data.append(data)