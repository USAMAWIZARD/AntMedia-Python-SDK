from hls import start_hls_pipeline
from PIL import Image
import numpy as np

i=0
def on_video_frame(numpy_frame):
    global i
    img = Image.fromarray(numpy_frame)
    img.save("./out/"+streamId+str(i)+".jpeg")
    print(numpy_frame.shape)
    i+=1
    img.close()


streamId = "test"

HLS_URL = "http://localhost:5080/LiveApp/streams/"+ streamId +".m3u8"
start_hls_pipeline(HLS_URL,on_video_frame)
