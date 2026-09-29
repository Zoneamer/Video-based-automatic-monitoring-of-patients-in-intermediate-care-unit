# ======================= IMPORTS ===========================
# Multithreading management to run multiple tasks simultaneously
import threading
# OpenCV for video processing and frame manipulation
import cv2
# Time management (sleep, perf_counter, etc)
import time
# Date and time management
from datetime import datetime
# Library to control the Raspberry Pi camera
from picamera2 import Picamera2
# TCP/UDP network communication
import socket
# File and folder management
import os
# Interaction with the Python interpreter
import sys
# Hashing to verify file integrity
import hashlib
# Custom configuration for this client
import config_client as config
# Management of failed files
import shutil

# Force automatic flush of print statements
sys.stdout.reconfigure(line_buffering = True) 


# ---------------------------- UDP CLIENT CLASS --------------------------
class UDPClient:
    def __init__(self, server_port = config.UDP_SERVER_PORT, timeout = config.UDP_TIMEOUT):
        '''
        Initialization of UDP client to communicate with the master
        '''
        self.server_address = (config.UDP_SERVER_IP, server_port)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.bind((config.UDP_SERVER_IP, server_port))
        self.ip = config.UDP_SERVER_IP
        self.adClient = None


    def send_command(self, command):
        '''
        Sends a command to the master via UDP
        '''
        if type(command) != bytes:
            commande = command.encode()
            print(f"{command} encoded ")
        else:
            commande = command
        try:
            self.server.sendto(commande, self.adClient)
            print(f"[UDP] Command : {command} sent to the Master")
        except Exception as e:
            print(f"[ERROR UDP] Unable to send command : {e}")


    def listen_command(self):
        '''
        Listens for a command received from the master
        '''
        (data, self.adClient) = self.server.recvfrom(1024)
        return data.decode()


    def close(self):
        '''
        Closes the UDP connection
        '''
        self.server.close()
        print("[UDP] Connexion closed.")


# ---------------------------- TCP CLIENT CLASS --------------------------
class TCPClient:
    def __init__(self, server_host = config.TCP_MASTER_IP, server_port = config.TCP_MASTER_PORT):
        '''
        Initialization of TCP client to send video files
        '''
        self.server_host = server_host
        self.server_port = server_port
        self.client_socket = None
        self.ip = config.UDP_SERVER_IP


    def connect(self):
        '''
        Establishes TCP connection with master server
        '''
        try:
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.client_socket.connect((self.server_host, self.server_port))
            print(f"[TCP] connected {self.server_host}:{self.server_port}")
        except Exception as e:
            print(f"[ERROR TCP] : {e}")
            self.client_socket = None
        return self.client_socket


    def send_file(self, file_path: str):
        '''
        Sends a video file in blocks with SHA256 verification
        '''
        BLOCK_SIZE = config.TCP_BLOCK_SIZE
        client_socket = self.client_socket
        if not self.client_socket:
            print("[ERROR] TCP not linked anymore.")
            return

        if not os.path.exists(file_path):
            print(f"[ERROR] File {file_path} does not exist")
            return
        
        print(f"[TCP] Sending file : {file_path}")
        try:
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(BLOCK_SIZE)
                    if not chunk:
                        # End of file, send chunk 0 to signal the end
                        while True:
                            client_socket.sendall((0).to_bytes(4, "big"))
                            self.client_socket.settimeout(0.5)
                            try:
                                ack = self.client_socket.recv(1)
                                if ack == b"1":
                                    break
                                
                            except socket.timeout:
                                pass
                            
                        break

                # Compute hash of the chunk for verification
                chunk_hash = hashlib.sha256(chunk).digest()
                length_bytes = len(chunk).to_bytes(4, "big")
                # Wait for acknowledgment
                retry = 0
                while retry < 5:
                    client_socket.sendall(length_bytes)
                    client_socket.sendall(chunk)
                    client_socket.sendall(chunk_hash)
                    self.client_socket.settimeout(0.5)
                    try:
                        # Wait for acknowledgment
                        ack = client_socket.recv(1)
                        if ack == b"1":
                            break
                        
                    except socket.timeout:
                        pass
                    
                    retry += 1
                    if retry >= 5:
                            raise Exception("Chunk rejected 5 times, aborting transfer")
                print(f"[TCP] File sent completely : {file_path}")
        except Exception as e :
            print(f"[ERROR TCP] Failed to send file {file_path} : {e}") 
            if client_socket :
                client_socket.close()
            # Move the failed file to another folder
            failed_folder = os.path.join(os.path.dirname(file_path), "failed_videos")
            os.makedirs(failed_folder, exist_ok = True)
            try : 
                shutil.move(file_path, failed_folder)
                print(f"[TCP] Incomplete file moved to failed_videos : {file_path}")
            except Exception as ee :
                print(f"[WARNING] Could not move failed file : {ee}")


# ---------------------------- CAMERA CLASS ------------------------------
class Camera:
    def __init__(self, host = config.TCP_MASTER_IP, fps = config.FPS, resolution = config.RESOLUTION,
                 save_dir = config.SAVE_DIR, segment_duration = config.SEGMENT_DURATION):
        '''
        Initialization of the camera and recording parameters
        '''
        self.segment_duration = segment_duration
        self.fps = fps
        self.resolution = resolution
        self.save_dir = os.path.expanduser(save_dir)
        os.makedirs(self.save_dir, exist_ok = True)
        self.ip = config.UDP_SERVER_IP
        self.udp_client = UDPClient()
        self.tcp_client = TCPClient()
        self.start = False
        self.camera = None
        self.video_writer = None
        self.recording = False
        self.last_order = None
        self.start_time = None
        self.video_path_list = []
        self.video_start = None
        self.video_start_info = None


    def start_camera(self):
        '''
        Initializes and starts the Picamera2 camera
        '''
        if self.camera is None:
            self.camera = Picamera2()
            self.camera.configure(self.camera.create_video_configuration(
                main={"size": self.resolution, "format": "RGB888"}))
            self.camera.start()
            self.camera.set_controls({"FrameRate": self.fps})
            print("[CAMERA] Camera has been launched", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))


    def create_new_segment(self):
        '''
        Creates a new video segment for recording
        '''
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
        self.video_path = os.path.join(self.save_dir, f"video_{timestamp}.mp4")
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        self.video_writer = cv2.VideoWriter(self.video_path, fourcc, self.fps, self.resolution)
        self.video_path_list.append(self.video_path)
        self.video_start = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        self.segment_start_perf = time.perf_counter()
        print(f"[CAMERA] New segment : {self.video_path} (starts at {self.video_start})")


    def start_recording(self):
        '''
        Starts video recording
        '''
        self.start_camera()
        self.create_new_segment()
        self.video_start_info = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        self.video_start = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        print(f"[CAMERA] Video starts (saved for trimm) at : {self.video_start_info}")


    def stop_recording(self):
        '''
        Stops video recording and releases resources
        '''
        if not self.recording:
            return
        
        self.recording = False
        time.sleep(0.2)
        if self.video_writer:
            try:
                self.video_writer.release()
                segment_end_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
                print(f"[Segment] Dernier segment {self.video_path} terminé à {segment_end_time}")
            except Exception as e:
                print(f"[CAMERA] Error releasing writer : {e}")
                
            self.video_writer = None
        if self.camera:
            try:
                self.camera.stop()
                self.camera.close()
                print("[CAMERA] Camera stopped and closed")
            except Exception as e:
                print(f"[CAMERA] Error closing camera {e}")
                
            self.camera = None
            cv2.destroyAllWindows()
        threading.Thread(target = self.send_videos, daemon = True).start()
        print("[CAMERA] Record stopped at : ", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))


    def send_videos(self):
        '''
        Sands all recorded videos to the TCP server
        After sending, deletes local files
        '''
        print("Sending videos started at :", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        self.tcp_client.connect()
        for path in self.video_path_list[:]:
            self.resample_video_to_fps(path)
            self.tcp_client.send_file(path)
            time.sleep(0.1)
            self.video_path_list.remove(path)
            try:
                os.remove(path)
                print("All videos have been sent at : ", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
            except:
                print("Not all videos have been sent")
                
        if self.tcp_client.client_socket:
            self.tcp_client.client_socket.close()
            self.tcp_client.client_socket = None
            print("[TCP] Connection closed after sending all videos")


    def record_loop(self):
        '''
        Main recording loop
        - Manages video segments
        - Corrects time drift
        - Writes frames to file
        '''
        DRIFT_CORRECTION_INTERVAL = config.DRIFT_CORRECTION_INTERVAL
        self.last_datetime_sync = datetime.now()
        self.last_perf_sync = time.perf_counter()
        while True:
            if self.start:
                print("Record loop start")
                if not self.recording:
                    self.recording = True
                    self.start_recording()
                    self.start_perf = time.perf_counter()
                    print("[Camera] RECORD Started")
                frame_index = 0
                frame_interval = 1 / self.fps
                segment_start_perf = time.perf_counter()
                segment_start_real = datetime.now()                
                next_drift_correction = time.perf_counter() + DRIFT_CORRECTION_INTERVAL
                while self.recording:
                    now_perf = time.perf_counter()
                    elapsed_segment = now_perf - segment_start_perf
                    if elapsed_segment >= self.segment_duration :
                        # End of segment, create a new segment
                        print("[Camera] Segment duration reached, new segment created")
                        segment_end_real = datetime.now()
                        real_elapsed = (segment_end_real - segment_start_real).total_seconds()
                        expected_duration = frame_index / self.fps
                        drift = real_elapsed - expected_duration
                        print(f"[Segment] Drift detected : {drift:.6f} s")
                        self.video_writer.release()
                        segment_end_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
                        print(f"[Segment] {self.video_path} finished at {segment_end_time}")
                        threading.Thread(target = self.send_videos, daemon = True).start()
                        self.create_new_segment()
                        # Reset counters and synchronization
                        frame_index = 0
                        self.start_perf = time.perf_counter()
                        self.last_perf_sync = self.start_perf
                        self.last_datetime_sync = datetime.now()
                        segment_start_perf = self.start_perf
                        segment_start_real = datetime.now()
                        next_drift_correction = self.start_perf + DRIFT_CORRECTION_INTERVAL
                        print("[RESET] Timing base reinitialized at new segment start\n")
                        continue
                    
                    # Capture a frame
                    frame = None
                    wait_start = time.perf_counter()
                    max_wait = config.MAX_FRAME_WAIT
                    while frame is None and (time.perf_counter() - wait_start) < max_wait:
                        frame = self.camera.capture_array()
                    if frame is not None:
                        self.video_writer.write(frame)
                    # Periodic drift correction
                    if time.perf_counter() >= next_drift_correction:
                        drift_sync = (
                            (datetime.now() - self.last_datetime_sync).total_seconds() 
                            - (time.perf_counter() - self.last_perf_sync)
                        )
                        self.start_perf += drift_sync
                        self.last_datetime_sync = datetime.now()
                        self.last_perf_sync = time.perf_counter()
                        next_drift_correction = time.perf_counter() + DRIFT_CORRECTION_INTERVAL
                        print(f"[SYNC] Drift corrected: {drift_sync:.6f}s")
                    # Calculate time to wait until next frame
                    next_frame_time = self.start_perf + (frame_index + 1) * frame_interval
                    delay = next_frame_time - time.perf_counter()
                    if delay > 0:
                        time.sleep(delay)
                    frame_index += 1
            else:
                self.stop_recording()
            time.sleep(1)


    def handle_command(self, command):
        '''
        Handles commands received from the master
        START / STOP / START_TIME
        '''
        try:
            print(f"Received command: {command}", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
            if command == "START" and not self.recording:
                self.start = True
                self.last_order = "START"
                self.udp_client.send_command(self.last_order)
            elif command == "STOP" and self.recording:
                self.start = False
                self.last_order = "STOP"
                self.udp_client.send_command(self.last_order)
                self.stop_recording()
            elif command == "START_TIME":
                # Send the start time back to the master
                self.udp_client.send_command(f"START_TIME:{self.video_start_info}")
                if self.recording:
                    print("Command STOP catched up")
                    self.start = False
                    self.last_order = "STOP"
                    self.udp_client.send_command(self.last_order)
                    self.stop_recording()
                print("-------------------------------------------------------------------------------------------")
            else:
                print("Command unknow or not apropriate")
        except Exception as e:
            print(f"{e}")


    def listen_udp(self):
        '''
        UDP command listening loop
        '''
        while True:
            try:
                data = self.udp_client.listen_command()
                self.handle_command(data.strip())
            except Exception as e:
                print(f"[ERREUR] Problem with UDP : {e}")
                
            time.sleep(0.01)


    def start_threads(self):
        '''
        Starts threads for UDP listening and continuous recording
        '''
        threading.Thread(target = self.listen_udp, daemon = False).start()
        threading.Thread(target = self.record_loop, daemon = False).start()


# ---------------------------- MAIN BLOCK ----------------------------
if __name__ == "__main__":
    # Print camera indentifier and launch time
    print(f"Camera {config.NUMERO} :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "\n")
    # Clean existing save directory
    for file in os.listdir(config.SAVE_DIR):
        if os.path.isfile(os.path.join(config.SAVE_DIR, file)):
            os.remove(os.path.join(config.SAVE_DIR, file))
    # Initialize camera and start threads
    cam = Camera()
    cam.start_threads()