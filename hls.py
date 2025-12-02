import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib
import numpy as np
import requests
import time

Gst.init(None)

callback = None
def on_decoded_video_buffer(pad, info, streamId):
    caps = pad.get_current_caps()
    structure = None
    if caps:
        structure = caps.get_structure(0)
        width = structure.get_int("width")[1]
        height = structure.get_int("height")[1]
        format = structure.get_string("format")
    else:
        print("No caps available")
        return Gst.FlowReturn.ERROR

    gst_buffer = info.get_buffer()

    if not gst_buffer:
        print("Unable to get GstBuffer ")
        return Gst.FlowReturn.ERROR

    (result, mapinfo) = gst_buffer.map(Gst.MapFlags.READ)
    assert result

    stride = width * 3 
    if structure and structure.has_field("stride"):
        stride = structure.get_int("stride")[1]
    else:
        # Calculate stride from buffer size if not explicitly provided
        # stride = buffer_size / height
        calculated_stride = mapinfo.size // height
        if calculated_stride >= width * 3:
            stride = calculated_stride
    
    expected_size = height * width * 3
    actual_size = mapinfo.size
    
    if stride != width * 3 or actual_size != expected_size:
        numpy_frame = np.zeros((height, width, 3), dtype=np.uint8)
        buffer_view = memoryview(mapinfo.data)
        
        for y in range(height):
            src_start = y * stride
            src_end = src_start + width * 3
            row_data = bytes(buffer_view[src_start:src_end])
            numpy_frame[y, :, :] = np.frombuffer(row_data, dtype=np.uint8).reshape(width, 3)
    else:
        numpy_frame = np.ndarray(
            shape=(height, width, 3),
            dtype=np.uint8,
            buffer=mapinfo.data)

    callback(numpy_frame)

    gst_buffer.unmap(mapinfo)

    return Gst.FlowReturn.OK

def url_exists(url, timeout=5):
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        return response.status_code == 200
    except requests.RequestException:
        return False

def start_hls_pipeline(url,on_video_frame_callback):
    while True:
        if url_exists(url):
            break
        print("waiting for hls url to be available")
        time.sleep(3)

    global callback

    callback = on_video_frame_callback
    pipeline_str = f"""
        souphttpsrc   location="{url}" !
        hlsdemux !
        decodebin  ! videoconvert ! capsfilter 
        caps=video/x-raw,format=RGB  !
        fakesink name=vsink sync=false
    """

    pipeline = Gst.parse_launch(pipeline_str)

    sink = pipeline.get_by_name("vsink")

    rgb_pad = sink.get_static_pad("sink")
    rgb_pad.add_probe(
        Gst.PadProbeType.BUFFER, on_decoded_video_buffer, "stream1")


    pipeline.set_state(Gst.State.PLAYING)
    print("Pipeline started…")

    # Run main loop
    loop = GLib.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        print("Stopping…")
        pipeline.set_state(Gst.State.NULL)


