import threading
import PyTango

GROUP = "test/group/1"
CONSUMER = "test/consumer/1"

STRATEGY = "read"
REPEAT = 1

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

group = PyTango.DeviceProxy(GROUP)
consumer = PyTango.DeviceProxy(CONSUMER)

group.write_attribute("strategy", STRATEGY)
consumer.write_attribute("strategy", STRATEGY)
for _ in xrange(REPEAT):
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