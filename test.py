import sys
import threading
import argparse

import PyTango

GROUP = "test/group/1"
CONSUMER = "test/consumer/1"

class Test(object):
    
    def __init__(self):
        self.state = PyTango.DevState.ON

    def push_event(self, event):
        if event.err:
            print event.errors
            return
        value = event.attr_value.value
        if (value == PyTango.DevState.ON and
            self.state == PyTango.DevState.MOVING):
            self.state_event.set()
        self.state = value
        
    def run(self, strategy, repeat):        
        group = PyTango.DeviceProxy(GROUP)
        consumer = PyTango.DeviceProxy(CONSUMER)

        group.write_attribute("strategy", strategy)
        consumer.write_attribute("strategy", strategy)
        
        for _ in xrange(repeat):
            id_ = consumer.subscribe_event("state",
                                           PyTango.EventType.CHANGE_EVENT,
                                           self.push_event)
            self.state = PyTango.DevState.ON
            self.state_event = threading.Event()
            consumer.Start()
            while True:
                if self.state_event.wait(0.01):
                    break
            consumer.unsubscribe_event(id_)
            if strategy == "event":
                assert consumer.read_attribute("event_order_ok").value == True
            print "data_time_sum ", consumer.read_attribute("data_time_sum").value

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("strategy", type=str)
    parser.add_argument("-r", "--repeat", type=int, default=1)
    args = parser.parse_args()
    strategy = args.strategy
    repeat = args.repeat
    
    test = Test()
    test.run(strategy, repeat) 
