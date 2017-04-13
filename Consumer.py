import sys
import time
import types
import threading
import numpy

import PyTango
from taurus.core.util.codecs import CodecFactory

class ConsumerThread(threading.Thread):

    def __init__(self, dev, strategy):
        super(ConsumerThread,self).__init__()
        self.dev = dev
        if strategy == "event":
            self.strategy = types.MethodType(ConsumerThread.event,
                                             self, ConsumerThread)
        elif strategy == "read":
            self.strategy = types.MethodType(ConsumerThread.read,
                                             self, ConsumerThread)

    def strategy(self):
        raise Exception("strategy is not known")

    def event(self):
        group = self.dev.group
        producers = self.dev.producers
        _stop_flag = self.dev._stop_flag
        state_event = self.dev.state_event

        data_ids = []
        for producer in producers:
            id_ = producer.subscribe_event("Data",
                                     PyTango.EventType.CHANGE_EVENT,
                                     self.dev)
            data_ids.append(id_)
        group.Start()
        while True:
            if _stop_flag or state_event.wait(0.01):
                break

        for producer, id_ in zip(producers, data_ids):
            producer.unsubscribe_event(id_)

    def read(self):
        group = self.dev.group
        producers = self.dev.producers
        _stop_flag = self.dev._stop_flag
        state_event = self.dev.state_event
        codec_obj = self.dev.codec_obj
        data_times = self.dev.data_times
        group.Start()
        while True:
            if _stop_flag or state_event.wait(0.01):
                break
            for producer in producers:
                t1 = time.time()
                value = producer.read_attribute("Data").value
                print value
                _ = codec_obj.decode(('json', value), ensure_ascii=True)
                print _
                t2 = time.time()
                data_times.append(t2 - t1)

    def pipe(self):
        pass

    def run(self):
        group = self.dev.group
        state_id = group.subscribe_event("State",
                                         PyTango.EventType.CHANGE_EVENT,
                                         self.dev)
        self.strategy()
        group.unsubscribe_event(state_id)
        self.dev.set_state(PyTango.DevState.ON)
        self.dev.push_change_event("state", PyTango.DevState.ON)


class ConsumerClass(PyTango.DeviceClass):

    cmd_list = { 'Start' : [ [ PyTango.ArgType.DevVoid, "" ],
                             [ PyTango.ArgType.DevVoid, "" ] ],
                 'Stop' : [ [ PyTango.ArgType.DevVoid, "" ],
                                     [ PyTango.ArgType.DevVoid, ""] ],
    }

    attr_list = { 'event_order_ok' : [ [ PyTango.ArgType.DevBoolean ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ] ],
                  'codec' : [ [ PyTango.ArgType.DevString ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
                  'data_time_avg' : [ [ PyTango.ArgType.DevDouble ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ] ],
                  'data_time_sum' : [ [ PyTango.ArgType.DevDouble ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ] ],
                  'strategy' : [ [ PyTango.ArgType.DevString ,
                                    PyTango.AttrDataFormat.SCALAR ,
                                    PyTango.AttrWriteType.READ_WRITE] ],
    }

    def __init__(self, name):
        PyTango.DeviceClass.__init__(self, name)
        self.set_type("TestDevice")


class Consumer(PyTango.Device_4Impl):

    #@PyTango.DebugIt()
    def __init__(self,cl,name):
        PyTango.Device_4Impl.__init__(self, cl, name)
        self.info_stream('In Consumer.__init__')
        self.codec = "json"
        self.codec_obj = CodecFactory().getCodec(self.codec)
        Consumer.init_device(self)

    @PyTango.DebugIt()
    def init_device(self):
        self.info_stream('In Python init_device method')
        self.set_change_event('state', True, False)
        self.set_state(PyTango.DevState.ON)
        self.push_change_event("state", PyTango.DevState.ON)
        self.state_event = None
        self._thread = None
        self._stop_flag = False
        self.data_order = 0
        self.state_order = 0
        self.data_times = []
        self.strategy = "event"
        db = PyTango.Util.instance().get_database()
        group_name = db.get_device_exported_for_class("Group").value_string[0]
        producers_names = db.get_device_exported_for_class("Producer").value_string
        self.group = PyTango.DeviceProxy(group_name)
        self.group_state = self.group.State()
        self.producers = [PyTango.DeviceProxy(name) for name in producers_names]

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
        self.state_event = threading.Event()
        self.data_times = []
        self.state_order = 0
        self.data_order = 0
        self.index = 1
        if self._thread and self._thread.isAlive():
            PyTango.Except.throw_exception('Busy', 
                                           'The previous command execution is still running', 
                                           'Start')
        else:
            self._stop_flag = False
            self.debug_stream('Starting thread...')
            self._thread = ConsumerThread(self, self.strategy)
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
    def read_event_order_ok(self, the_att):
        self.info_stream("read_event_order_ok")
        value = False
        len_producers = len(self.producers)
        if (self.data_order == len_producers + len_producers and
            self.state_order == (len_producers + len_producers + 1)):
            value = True
        the_att.set_value(value)

    @PyTango.DebugIt()
    def is_event_order_ok_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)
    
    @PyTango.DebugIt()
    def read_codec(self, attr):
        attr.set_value(self.codec)

    @PyTango.DebugIt()
    def write_codec(self, attr):
        self.codec = attr.get_write_value()
        self.codec_obj = CodecFactory().getCodec(self.codec)

    @PyTango.DebugIt()
    def is_codec_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)

    @PyTango.DebugIt()
    def read_data_time_avg(self, the_att):
        self.info_stream("read_data_time_avg")
        value = numpy.mean(self.data_times)
        the_att.set_value(value)

    @PyTango.DebugIt()
    def is_data_time_avg_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)
    
    @PyTango.DebugIt()
    def read_data_time_sum(self, the_att):
        self.info_stream("read_data_time_sum")
        value = numpy.sum(self.data_times)
        the_att.set_value(value)

    @PyTango.DebugIt()
    def is_data_time_sum_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)

    @PyTango.DebugIt()
    def read_strategy(self, attr):
        attr.set_value(self.strategy)

    @PyTango.DebugIt()
    def write_strategy(self, attr):
        self.strategy = attr.get_write_value()

    @PyTango.DebugIt()
    def is_strategy_allowed(self, req_type):
        return self.get_state() in (PyTango.DevState.ON,)

    def push_event(self, event):
        t1 = time.time()
        if event.err:
            if event.errors[0].reason != "UnsupportedFeature":
                print event.errors
            return
        value = event.attr_value.value
        if event.attr_name.endswith("data"):
            _ = self.codec_obj.decode(('json', value), ensure_ascii=True)
            t2 = time.time()
            self.data_times.append(t2 - t1)
            self.data_order = self.index
            self.index += 1
        elif event.attr_name.endswith("state"):
            if (value == PyTango.DevState.ON and
                self.group_state == PyTango.DevState.MOVING):
                self.state_order = self.index
                self.index += 1
                self.state_event.set()
            self.group_state = value

if __name__ == '__main__':
    util = PyTango.Util(sys.argv)
    util.add_class(ConsumerClass, Consumer)

    U = PyTango.Util.instance()
    U.server_init()
    U.server_run()

