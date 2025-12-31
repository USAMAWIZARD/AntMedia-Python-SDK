# ulimit -n 65536 run this command to increase socket opening limit
import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstWebRTC', '1.0')
gi.require_version('GstSdp', '1.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import Gst, GstSdp, GstWebRTC, GstVideo, GLib
import threading
import json
import sys
import os
import asyncio
import ssl
import time
import random
import websocket

PIPELINE_DESC_SEND = '''
 videotestsrc ! videoconvert  ! video/x-raw,format=I420 ! x264enc  speed-preset=veryfast tune=zerolatency  ! rtph264pay !
 queue ! application/x-rtp,media=video,encoding-name=H264,payload=96 ! webrtcbin name=sendrecv  bundle-policy=max-bundle
 audiotestsrc ! audioconvert ! audioresample   ! opusenc bitrate=192000  ! queue ! rtpopuspay ! application/x-rtp,media=audio,encoding-name=OPUS,payload=97 ! sendrecv. '''

PIPELINE_DESC_RECV = '''webrtcbin name=sendrecv  bundle-policy=max-bundle fakesrc ! fakesink'''


class WebRTCAdapter:
    webrtc_clients = {}  # idmode stream1play

    def __init__(self, URL):
        self.server = URL
        self.client_id = ""

    def on_message(self, ws, message):
        data = json.loads(message)

        print('Message: ' + data['command'], data)

        if (data['command'] == 'start'):
            self.start_publishing(data["streamId"])
        elif (data['command'] == 'takeCandidate'):
            self.take_candidate(data)
        elif (data['command'] == 'takeConfiguration'):
            self.take_configuration(data)
        elif (data['command'] == 'notification'):
            self.notification(data)
        elif (data['command'] == 'error'):
            print('Message: ' + data['definition'])

    def on_error(self, ws, error):
        print("Client {} error: {}".format(self.client_id, error))

    def on_close(self, ws, close_status_code, close_msg):
        print("Client {} closed: {}, {}", self.client_id,
              close_status_code, close_msg)

    def on_open(self, ws):
        self.isopen.set()
        print(self.ws_conn)

    def get_websocket(self):
        return self.ws_conn

    def socket_listner_thread(self):
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp(
            self.server,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

        self.ws_conn = ws
        ws.run_forever()

    def send_ping(self):
        self.ws_conn.send('{"command": "ping"}')
        pingtimer = threading.Timer(5, self.send_ping)
        pingtimer.start()

    def connect(self):
        self.callback = "test"
        self.isopen = threading.Event()
        thread = threading.Thread(
            target=self.socket_listner_thread, args=())
        thread.daemon = True

        pingtimer = threading.Timer(5, self.send_ping)
        pingtimer.start()

        thread.start()
        self.isopen.wait()

    def play(self, id, on_video_callback=None, on_audio_callback=None):
        print("play request sent for id", id)
        wrtc_client_id = id
        if self.wrtc_client_exist(id):
            pass
        else:
            play_client = WebRTCClient(
                id, "play", self.ws_conn, on_video_callback, on_audio_callback)
            WebRTCAdapter.webrtc_clients[wrtc_client_id] = play_client
            play_client.play()

    def start_publishing(self, id):
        if publish_client := self.get_webrtc_client(id):
            publish_client.start_pipeline("publish")
        else:
            print("no client found")

    def publish(self, id):
        wrtc_client_id = id
        if wrtc_client_id in WebRTCAdapter.webrtc_clients:
            pass
        else:
            publish_client = WebRTCClient(id, "publish", self.ws_conn)
            WebRTCAdapter.webrtc_clients[wrtc_client_id] = publish_client
            publish_client.send_publish_request()

    def wrtc_client_exist(self, id):
        return id in WebRTCAdapter.webrtc_clients

    def get_webrtc_client(self, id):
        if id in WebRTCAdapter.webrtc_clients:
            return WebRTCAdapter.webrtc_clients[id]
        return None

    def take_candidate(self, candidate):
        stream_id = candidate["streamId"]
        webrtc_client = self.get_webrtc_client(stream_id)
        if webrtc_client:
            webrtc_client.take_candidate(candidate)
        else:
            print("no webrtc client exist for this request", stream_id)

    def take_configuration(self, config):
        wrtc_client_id = config["streamId"]

        wrtc_client = self.get_webrtc_client(wrtc_client_id)
        print(wrtc_client, wrtc_client_id, WebRTCAdapter.webrtc_clients)
        if wrtc_client:
            wrtc_client.take_configuration(config)
        else:
            print("no webrtc client exist for this request",
                  wrtc_client_id)

    def notification(self, data):
        if (data['definition'] == 'publish_started'):
            print('Publish Started')
        else:
            print(data['definition'])


class WebRTCClient():

    def __init__(self, id, mode, ws_client, on_video_callback=None, on_audio_callback=None):
        self.id = id
        self.pipe = None
        self.webrtc = None
        self.peer_id = None
        self.mode = mode
        self.websocket_client = ws_client
        self.on_video_callback = on_video_callback
        self.on_audio_callback = on_audio_callback
        self.processed_pads = set()  # Track processed pads to avoid duplicates
        self.early_candidates = []

    def send_publish_request(self):
        self.websocket_client.send(
            '{"command":"publish","streamId":"' + self.id + '", "token":"null","video":true,"audio":true}')

    def play(self):
        self.websocket_client.send(
            '{"command":"play","streamId":"' + self.id + '", "token":"null"}')

    def send_sdp(self, sdp, type):
        print('Send SDP ' + type, self.id)
        sdp = sdp.as_text()
        self.websocket_client.send(
            '{"command":"takeConfiguration", "streamId": "' + self.id + '", "type": "' + type + '", "sdp": ' + json.dumps(sdp) + '}')

    def on_negotiation_needed(self, element):
        print('Negotiation Needed')
        promise = Gst.Promise.new_with_change_func(
            self.on_offer_created, element, None)
        element.emit('create-offer', None, promise)

    def send_ice_candidate_message(self, _, mlineindex, candidate):
        data = '{"command":"takeCandidate","streamId":"' + self.id + '","label":' + \
            str(mlineindex) + ', "id":"' + str(mlineindex) + \
            '", "candidate":"' + str(candidate) + '"}'
        self.websocket_client.send(data)

    def on_incoming_decodebin_stream(self, _, pad):
        print('Incoming Decodebin Stream')
        if not pad.has_current_caps():
            print(pad, 'has no caps, ignoring')
            return
        caps = pad.get_current_caps()
        s = caps
        name = s.to_string()

        if name.startswith('video'):
            print("video stream recieved", self.id)
            q = Gst.ElementFactory.make('queue')
            conv = Gst.ElementFactory.make('videoconvert')
            sink = Gst.ElementFactory.make('autovideosink')

            self.pipe.add(q)
            self.pipe.add(conv)
            self.pipe.add(sink)
            self.pipe.sync_children_states()
            pad.link(q.get_static_pad('sink'))

            q.link(conv)
            conv.link(sink)

        elif name.startswith('audio'):
            print("audio stream recieved")
            q = Gst.ElementFactory.make('queue')
            conv = Gst.ElementFactory.make('audioconvert')
            resample = Gst.ElementFactory.make('audioresample')
            sink = Gst.ElementFactory.make('autoaudiosink')

            self.pipe.add(q)
            self.pipe.add(conv)
            self.pipe.add(resample)
            self.pipe.add(sink)

            self.pipe.sync_children_states()
            pad.link(q.get_static_pad('sink'))
            q.link(conv)
            conv.link(resample)
            resample.link(sink)

    def handle_media_stream(self, pad, gst_pipe, convert_name, sink_name):
        caps = pad.get_current_caps()
        name = caps.get_structure(0).get_name()
        print("handle_media_stream: handling caps {} with {} and {}".format(name, convert_name, sink_name))

        if name.startswith("video") and convert_name != "videoconvert":
            return
        if name.startswith("audio") and convert_name != "audioconvert":
            return

        # Create a queue and sink element
        queue = Gst.ElementFactory.make("queue", None)
        sink = Gst.ElementFactory.make(sink_name, None)
        converter = Gst.ElementFactory.make(convert_name, None)

        if not all([queue, sink, converter]):
            print("Failed to create necessary GStreamer elements.")
            return

        # Safely set properties if they exist
        if sink.find_property("sync"):
            sink.set_property("sync", False)
        if sink.find_property("async"):
            sink.set_property("async", False)

        if convert_name == "audioconvert":
            print("Audio stream detected")
            resample = Gst.ElementFactory.make("audioresample", None)

            if not resample:
                print("Failed to create audioresample element.")
                return

            gst_pipe.add(queue)
            gst_pipe.add(converter)
            gst_pipe.add(resample)
            gst_pipe.add(sink)

            queue.sync_state_with_parent()
            converter.sync_state_with_parent()
            resample.sync_state_with_parent()
            sink.sync_state_with_parent()

            queue.link(converter)
            converter.link(resample)
            resample.link(sink)
            if self.on_audio_callback:
                pass
                # pad.add_probe(
                #     Gst.PadProbeType.BUFFER, self.on_audio_callback, self.id)

        else:
            print("Video stream detected")

            gst_pipe.add(queue)
            gst_pipe.add(converter)
            gst_pipe.add(sink)

            queue.sync_state_with_parent()
            converter.sync_state_with_parent()
            sink.sync_state_with_parent()
            queue.link(converter)

            if self.on_video_callback:
                capsfilter = Gst.ElementFactory.make("capsfilter")
                rgbcaps = Gst.caps_from_string("video/x-raw,format=RGB")
                capsfilter.set_property("caps", rgbcaps)
                gst_pipe.add(capsfilter)
                capsfilter.sync_state_with_parent()

                rgb_pad = sink.get_static_pad("sink")
                converter.link(capsfilter)
                capsfilter.link(sink)

                rgb_pad.add_probe(
                    Gst.PadProbeType.BUFFER, self.on_video_callback, self.id)
            else:
                converter.link(sink)

        queue_sink_pad = queue.get_static_pad("sink")
        if not queue_sink_pad:
            print("Failed to get the sink pad of the queue.")
            return

        ret = pad.link(queue_sink_pad)
        if ret != Gst.PadLinkReturn.OK:
            print("Failed to link pad: {}".format(ret))
        else:
            print("Pad successfully linked.")

    def on_bus_message(self, bus, message):
        t = message.type
        if t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            print("Error from element {}: {}".format(message.src.get_name(), err.message))
            if debug:
                print("Debug info: {}".format(debug))
        elif t == Gst.MessageType.EOS:
            print("End-of-stream")
        elif t == Gst.MessageType.STATE_CHANGED:
            if isinstance(message.src, Gst.Pipeline):
                old, new, pending = message.parse_state_changed()
                print("Pipeline {} state changed from {} to {}".format(self.id, old.value_nick, new.value_name))
        elif t == Gst.MessageType.LATENCY:
            self.pipe.recalculate_latency()
        elif t == Gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            print("Warning from element {}: {}".format(message.src.get_name(), err.message))

    def on_incoming_stream(self, webrtc, pad):
        pad_name = pad.get_name()
        print("on_incoming_stream called for pad: {}, stream_id: {}".format(pad_name, self.id))
        
        # Only process source pads
        if pad.get_direction() != Gst.PadDirection.SRC:
            return
        
        # Check if pad has caps
        if not pad.has_current_caps():
            print("Pad {} has no caps yet, setting up caps probe".format(pad.get_name()))
            pad.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, self.on_pad_caps, webrtc)
            return
        
        caps = pad.get_current_caps()
        if not caps:
            return
        
        structure = caps.get_structure(0)
        mediatype = structure.get_value("media")
        
        if not mediatype:
            return

        # Check if we've already processed this pad for this media type
        if pad_name in self.processed_pads:
            return
        self.processed_pads.add(pad_name)

        print("Processing incoming stream - media type: {}, stream_id: {}".format(mediatype, self.id))
        
        depay = None
        if mediatype.startswith("video"):
            depay = Gst.ElementFactory.make("rtph264depay", None)
            convert_name = "videoconvert"
            sink_name = "xvimagesink"
        elif mediatype.startswith("audio"):
            depay = Gst.ElementFactory.make("rtpopusdepay", None)
            convert_name = "audioconvert"
            sink_name = "autoaudiosink"
        else:
            print("Unknown pad {}, ignoring".format(pad.get_name()))
            return

        if self.on_video_callback and mediatype.startswith("video"):
            sink_name = "fakesink"

        # Create elements
        queue = Gst.ElementFactory.make("queue", None)
        # Use a larger queue for video to handle jitter better
        if mediatype.startswith("video"):
            queue.set_property("max-size-buffers", 200)
            queue.set_property("max-size-time", 0)
            queue.set_property("max-size-bytes", 0)
        
        decodebin = Gst.ElementFactory.make("decodebin", None)
        decodebin.connect("pad-added", self.on_decodebin_pad_added, [convert_name, sink_name])
        
        if not all([queue, depay, decodebin]):
            print("Failed to create GStreamer elements")
            return

        pipeline = self.pipe
        pipeline.add(queue)
        pipeline.add(depay)
        pipeline.add(decodebin)

        # Link webrtcbin pad -> queue -> depay -> decodebin
        # (webrtcbin handles jitter internally via its latency property)
        queue_sink = queue.get_static_pad("sink")
        ret = pad.link(queue_sink)
        if ret != Gst.PadLinkReturn.OK:
            print("Failed to link incoming pad {} to queue: {}".format(pad.get_name(), ret))
            return
        
        queue.link(depay)
        depay.link(decodebin)

        for element in [queue, depay, decodebin]:
            element.sync_state_with_parent()

    def on_decodebin_pad_added(self, decodebin, pad, data):
        convert_name, sink_name = data
        if not pad.has_current_caps():
            pad.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, self.on_pad_caps_decodebin, data)
            return

        self.handle_media_stream(pad, self.pipe, convert_name, sink_name)

    def on_pad_caps_decodebin(self, pad, info, user_data):
        event = info.get_event()
        if event and event.type == Gst.EventType.CAPS:
            print("Caps available on decodebin pad: {}".format(pad.get_name()))
            # Run this in the next iteration of the loop to avoid recursion issues
            GLib.idle_add(self.handle_media_stream, pad, self.pipe, user_data[0], user_data[1])
            return Gst.PadProbeReturn.REMOVE
        return Gst.PadProbeReturn.OK

    def on_pad_caps(self, pad, info, user_data):
        """Callback for when caps become available on a pad"""
        event = info.get_event()
        if event and event.type == Gst.EventType.CAPS:
            caps = event.get_structure()
            if caps:
                print("Caps now available on pad: {}, processing stream".format(pad.get_name()))
                # Process the stream now that caps are available
                # user_data should be the webrtc element that was passed when setting up the probe
                webrtc = user_data if user_data else self.webrtc
                self.on_incoming_stream(webrtc, pad)
                return Gst.PadProbeReturn.REMOVE
        return Gst.PadProbeReturn.OK

    def start_pipeline(self, mode):
        print('Creating WebRTC Pipeline', mode, self.id)
        if mode == "publish":
            self.pipe = Gst.parse_launch(PIPELINE_DESC_SEND)
            self.webrtc = self.pipe.get_by_name('sendrecv')
        else:
            # Create a pipeline and add webrtcbin to it
            self.webrtc = Gst.ElementFactory.make("webrtcbin", "sendrecv")
            # Set internal jitter buffer latency on webrtcbin
            self.webrtc.set_property("latency", 500) # Back to 500ms for stability
            self.webrtc.set_property("bundle-policy", "max-bundle")
            self.pipe = Gst.Pipeline.new("pipeline-" + self.id)
            self.pipe.add(self.webrtc)

        if not self.webrtc:
            print("Failed to create or find webrtcbin element for stream {}".format(self.id))
            return

        if mode == "publish":
            self.webrtc.connect('on-negotiation-needed',
                                self.on_negotiation_needed)
        
        self.webrtc.connect('on-ice-candidate',
                            self.send_ice_candidate_message)
        self.webrtc.connect('pad-added', self.on_incoming_stream)
        
        bus = self.pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self.on_bus_message)
        
        self.pipe.set_state(Gst.State.PLAYING)
        
        # Apply any candidates that arrived early
        for cand in self.early_candidates:
            self.webrtc.emit('add-ice-candidate', cand['label'], cand['candidate'])
        self.early_candidates = []

    def take_candidate(self, data):
        if self.webrtc:
            self.webrtc.emit('add-ice-candidate', data['label'], data['candidate'])
        else:
            print("Queueing candidate for stream {}".format(self.id))
            self.early_candidates.append(data)

    def on_offer_created(self, promise, _, __):

        print('Offer Created')
        promise.wait()
        reply = promise.get_reply()
        # Please check -> https://github.com/centricular/gstwebrtc-demos/issues/42
        offer = reply.get_value('offer')
        promise = Gst.Promise.new()
        self.webrtc.emit('set-local-description', offer, promise)
        promise.interrupt()
        self.send_sdp(offer.sdp, "offer")

    def on_answer_created(self, promise, _, __):

        print("answer created")
        promise.wait()
        reply = promise.get_reply()
        answer = reply.get_value('answer')
        promise = Gst.Promise.new()
        self.webrtc.emit('set-local-description', answer, promise)
        promise.interrupt()
        self.send_sdp(answer.sdp, "answer")

    def on_description_set(self, promise, _, __):
        res = promise.wait()
        if res != Gst.PromiseResult.REPLIED:
            print("Failed to set description for stream {}".format(self.id))
        else:
            print("Successfully set description for stream {}".format(self.id))

    def take_configuration(self, data):
        if (data['type'] == 'answer'):
            if not self.webrtc:
                print("Cannot set remote answer: webrtcbin not initialized for stream {}".format(self.id))
                return
            res, sdpmsg = GstSdp.SDPMessage.new()
            GstSdp.sdp_message_parse_buffer(
                bytes(data['sdp'].encode()), sdpmsg)
            answer = GstWebRTC.WebRTCSessionDescription.new(
                GstWebRTC.WebRTCSDPType.ANSWER, sdpmsg)
            promise = Gst.Promise.new_with_change_func(self.on_description_set, None, None)
            self.webrtc.emit('set-remote-description', answer, promise)

        elif (data['type'] == 'offer'):
            if self.pipe is None:
                self.start_pipeline("play")
            
            if not self.webrtc:
                print("Cannot set remote offer: webrtcbin not initialized for stream {}".format(self.id))
                return

            res, sdpmsg = GstSdp.SDPMessage.new()
            GstSdp.sdp_message_parse_buffer(
                bytes(data['sdp'].encode()), sdpmsg)
            offer = GstWebRTC.WebRTCSessionDescription.new(
                GstWebRTC.WebRTCSDPType.OFFER, sdpmsg)
            
            # Chain set-remote-description and create-answer
            promise = Gst.Promise.new_with_change_func(self.on_offer_set, self.webrtc, None)
            self.webrtc.emit('set-remote-description', offer, promise)

    def on_offer_set(self, promise, element, _):
        reply = promise.wait()
        if reply != Gst.PromiseResult.REPLIED:
            print("Failed to set remote offer for stream {}".format(self.id))
            return
        
        print("Remote offer set, creating answer for stream {}".format(self.id))
        answer_promise = Gst.Promise.new_with_change_func(
            self.on_answer_created, element, None)
        element.emit("create-answer", None, answer_promise)

    def close_pipeline(self):
        print('Close Pipeline')
        self.pipe.set_state(Gst.State.NULL)
        self.pipe = None
        self.webrtc = None

    def stop(self):
        if self.websocket_client:
            self.websocket_client.close()


def init_gstreamer():
    Gst.init(None)
    # Gst.debug_set_default_threshold(3)
    if not check_plugins():
        sys.exit(1)


def check_plugins():
    needed = ["opus", "vpx", "nice", "webrtc", "dtls", "srtp", "rtp",
              "rtpmanager", "videotestsrc", "audiotestsrc"]
    missing = list(
        filter(lambda p: Gst.Registry.get().find_plugin(p) is None, needed))
    if len(missing):
        print('Missing gstreamer plugins:', missing)
        return False
    return True
