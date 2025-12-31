from webrtc import WebRTCAdapter
from webrtc import init_gstreamer
import requests
import time
from gi.repository import GLib

appname = "LiveApp"
WEBSOCKET_URL = 'ws://localhost:5080/' + appname + '/websocket'
prefix = "myStream-"

loop = None


def publish_test(num_streams):

    for i in range(num_streams):
        webrtc_adapter = WebRTCAdapter(WEBSOCKET_URL)
        webrtc_adapter.connect()
        webrtc_adapter.publish(prefix + str(i))
        time.sleep(0.5) # Add a small delay


def play_test(num_streams):

    webrtc_adapter = WebRTCAdapter(WEBSOCKET_URL)
    webrtc_adapter.connect()
    for i in range(num_streams):
        webrtc_adapter.play(prefix + str(i))
        time.sleep(0.1) # Add a small delay


def wait_for_publish(streamlist):
    active_stream = get_all_active_streams(appname)
    print("stream list", streamlist)

    for stream in active_stream:
        if stream in streamlist:
            streamlist.remove(stream)

    time.sleep(1)
    if (len(streamlist) > 0):
        print("waiting for all the streams to get published")
        wait_for_publish(streamlist)
    else:
        print("all streams are published")


def get_all_active_streams(appname):
    if appname == []:
        return None
    # Define the API endpoint and headers
    url = 'https://test.antmedia.io:5443/' + \
        appname + '/rest/v2/broadcasts/list/0/100'
    headers = {
        'accept': 'application/json',
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        print("Data retrieved successfully:")

        broadcasting_stream_ids = [
            stream['streamId'] for stream in response_data
            if stream['status'] == 'broadcasting'
        ]
        print(broadcasting_stream_ids)
        return broadcasting_stream_ids
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print("Response:", response.text)


# ulimit -n 65536 run this command to increase socket opening limit

def main():
    init_gstreamer()

    nbstreams = 100
    play_test(nbstreams)

    loop = GLib.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.quit()


main()
