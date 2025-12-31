import argparse
from webrtc import WebRTCAdapter, init_gstreamer
from gi.repository import GLib

parser = argparse.ArgumentParser(description="WebRTC Stream Player/Publisher")
parser.add_argument("websocket_url", help="Full WebSocket URL (e.g., ws://ip:port/appname/websocket)")
parser.add_argument("stream_id", help="Stream ID to play or publish")
parser.add_argument("mode", choices=["play", "publish"], help="Mode: 'play' to play a stream, 'publish' to publish a stream")

args = parser.parse_args()

init_gstreamer()
webrtc_adapter = WebRTCAdapter(args.websocket_url)
webrtc_adapter.connect()

if args.mode == "play":
    webrtc_adapter.play(args.stream_id)
elif args.mode == "publish":
    webrtc_adapter.publish(args.stream_id)

loop = GLib.MainLoop()
try:
    loop.run()
except KeyboardInterrupt:
    loop.quit()
