import requests

yolo_cfg_url = "https://raw.githubusercontent.com/pjreddie/darknet/master/cfg/yolov3.cfg"
yolo_weights_url = "https://pjreddie.com/media/files/yolov3.weights"
coco_names_url = "https://raw.githubusercontent.com/pjreddie/darknet/master/data/coco.names"

yolo_cfg_file = "yolov3.cfg"
yolo_weights_file = "yolov3.weights"
coco_names_file = "coco.names"

def download_file(url, filename):
    response = requests.get(url)
    with open(filename, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded {filename}")

download_file(yolo_cfg_url, yolo_cfg_file)
download_file(yolo_weights_url, yolo_weights_file)
download_file(coco_names_url, coco_names_file)