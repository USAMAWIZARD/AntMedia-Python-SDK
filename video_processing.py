from webrtc import WebRTCAdapter, init_gstreamer
from gi.repository import Gst
import numpy as np
from PIL import Image
import time

appname = "LiveApp"
WEBSOCKET_URL = 'ws://127.0.0.0:5080/' + appname + '/websocket'
last_pts = None
pts_count = 0

def on_decoded_audio_buffer(pad, info, u_data):
    gst_buffer = info.get_buffer()
    if not gst_buffer:
        print("Unable to get GstBuffer ")
        return

    if not gst_buffer:
        print("failed to get buffer")

    (result, mapinfo) = gst_buffer.map(Gst.MapFlags.READ)

    assert result
    return Gst.PadProbeReturn.OK


def on_decoded_video_buffer(pad, info, streamId):
    caps = pad.get_current_caps()
    if caps:
        structure = caps.get_structure(0)
        width = structure.get_int("width")[1]
        height = structure.get_int("height")[1]
        format = structure.get_string("format")
        print(width, height, streamId, format)

    gst_buffer = info.get_buffer()

    if not gst_buffer:
        print("Unable to get GstBuffer ")

    (result, mapinfo) = gst_buffer.map(Gst.MapFlags.READ)
    assert result

    numpy_frame = np.ndarray(
        shape=(height, width, 3),
        dtype=np.uint8,
        buffer=mapinfo.data)

    img = Image.fromarray(numpy_frame)
    img.save(streamId+".jpeg")
    img.close()

    gst_buffer.unmap(mapinfo)

    return Gst.FlowReturn.OK

init_gstreamer()
webrtc_adapter = WebRTCAdapter(WEBSOCKET_URL)
webrtc_adapter.connect()  # replace publish call with play to play the stream
# pass none in audio or video callbacks if you are not interested in them

webrtc_adapter.play("test", on_decoded_video_buffer,
                    on_decoded_audio_buffer)
# set above callback to none if you don't want any of them
