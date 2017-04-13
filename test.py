import sys
import threading
import PyTango

GROUP = "test/group/1"
CONSUMER = "test/consumer/1"

state = PyTango.DevState.ON

def push_event(event):
    global state
    if event.err:
        print event.errors
        return
    value = event.attr_value.value
    if (value == PyTango.DevState.ON and
        state == PyTango.DevState.MOVING):
        state_event.set()
    state = value

try:
    strategy = sys.argv[1]
except IndexError:
    strategy = "read"
else:
    try:
        repeat = int(sys.argv[2])
    except IndexError:
        repeat = 1
        
group = PyTango.DeviceProxy(GROUP)
consumer = PyTango.DeviceProxy(CONSUMER)

group.write_attribute("strategy", strategy)
consumer.write_attribute("strategy", strategy)

for _ in xrange(repeat):
    id_ = consumer.subscribe_event("state",
                                   PyTango.EventType.CHANGE_EVENT,
                                   push_event)
    state = PyTango.DevState.ON
    state_event = threading.Event()
    consumer.Start()
    while True:
        if state_event.wait(0.01):
            break
    consumer.unsubscribe_event(id_)
    if STRATEGY == "event":
        assert consumer.read_attribute("event_order_ok").value == True