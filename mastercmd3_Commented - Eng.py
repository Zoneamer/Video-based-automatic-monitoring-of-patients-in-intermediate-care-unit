# ======================= IMPORTS ===========================
# Time management (sleep, perf_counter, etc)
import time
# File and folder management
import os
# Multithreading management
import threading
# TCP/UDP network communication
import socket
# To listen to multiple sockets simultaneously
import select
# Date and time management
from datetime import datetime
# Hashing to verify file integrity
import hashlib  
# Raspberry Pi GPIO management
import RPi.GPIO as GPIO
import sys
# Force automatic flushing of prints
sys.stdout.reconfigure(line_buffering = True)
# OpenCV for video processing
import cv2
# Execution of external commands (e.g., ffmpeg)
import subprocess
# Regular expressions
import re
# Array / image manipulation
import numpy as np
# Custom project configuration
import config_master as config

# Global index to name videos
record_index = 1


# ---------------------------- LED CLASS ---------------------------------

class Led:
    def __init__(self, pin = config.LED_PIN):
        '''
        LED initialization on the chosen GPIO
        '''
        import RPi.GPIO as GPIO 
        self.GPIO = GPIO
        self.pin = pin
        self.GPIO.setmode(GPIO.BCM)
        self.GPIO.setup(self.pin, GPIO.OUT)
        self._blinking = False
        self._blink_thread = None


    def on(self):
        '''
        Turn the LED on 
        '''
        self._blinking = False
        self.GPIO.output(self.pin, self.GPIO.HIGH)


    def off(self):
        '''
        Turn the LED off
        '''
        self._blinking = False
        self.GPIO.output(self.pin, self.GPIO.LOW)


    def toggle(self):
        '''
        Toggle the current state of the LED
        '''
        current = self.GPIO.input(self.pin)
        self.GPIO.output(self.pin, not current)


    def blink(self, frequency = config.BLINK_FREQUENCY):
        '''
        Make the LED blink at a given frequency
        '''
        if self._blinking:
            self.stop_blink()
        self._blinking = True


        def _blink_loop():
            delay = 1 / (2 * frequency)
            while self._blinking:
                self.toggle()
                time.sleep(delay)
            self.GPIO.output(self.pin, self.GPIO.LOW)


        import threading
        self._blink_thread = threading.Thread(target = _blink_loop, daemon = True)
        self._blink_thread.start()


    def stop_blink(self):  
        '''
        Stop blinking and turn off the LED
        '''
        self._blinking = False
        if self._blink_thread and self._blink_thread.is_alive():
            self._blink_thread.join(timeout = 0.1)
        self._blink_thread = None
        self.GPIO.output(self.pin, self.GPIO.LOW)


    def cleanup(self):
        '''
        Clean up the GPIO
        '''
        self.stop_blink()
        self.GPIO.cleanup(self.pin)


# ---------------------------- TCP SERVER CLASS --------------------------

class TCPServer:
    def __init__(self, server_host = config.MASTER_IP, server_port = config.TCP_PORT, video_path = os.path.join(config.PATH_ROOT, config.VIDEO_FOLDER_NAME)):
        '''
        TCP server initialization to receive videos
        '''
        self.server_host = server_host
        self.server_port = server_port
        self.video_path = video_path
        self.connected = [] # List of connected clients
        self.error_count = 0 # Reception error counter
        self.saved = False # Flag if the video is saved
        self.clientip = None

		
    def receive_file(self, server_socket, folder_path, client_ip):
        '''
        Receive a video in multiple blocks with SHA256 verification
        '''
        global record_index
        self.saved = False
        i = 0
        os.makedirs(folder_path, exist_ok = True)
        # Find the last file index to increment the name correctly
        existing_files = [ 
            f for f in os.listdir(folder_path)
            if f.lower().endswith(".mp4") and "_temp" not in f 
        ]
        max_subindex = 0
        for f in existing_files:
            match = re.search(rf"video{record_index}_(\d+)?\.mp4$", f)
            if match:
                num = int(match.group(1))
                if num > max_subindex:
                    max_subindex = num
        next_subindex = max_subindex + 1
        final_filename = f"video{record_index}_{next_subindex}.mp4"
        temp_file = os.path.join(folder_path, f"{final_filename}_temp")
        print(f"[TCP] [{client_ip}] Reception of {final_filename}...")
        try :
            with open(temp_file, 'wb') as file:
                while True:
                    try:
                        # Reading the block size (4 bytes)
                        raw_len = recv_exact(server_socket, 4)
                        if raw_len is None:
                            break

                        block_len = int.from_bytes(raw_len, 'big')
                        if block_len <= 0 : 
                            server_socket.sendall(b"1")
                            break
                        
                        # Reading the entire data block
                        chunk = b''
                        while len(chunk) < block_len:
                            packet = recv_exact(server_socket, (block_len - len(chunk)))
                            if not packet:
                                raise ConnectionResetError("[TCP] Client disconnected mid-chunk")
                               
                            chunk += packet
                        if len(chunk) != block_len:
                            print("[TCP] Incomplete block, stopping...")
                            break
 
                        # Reading and verifying the SHA256 hash
                        received_hash = recv_exact(server_socket, 32)
                        if received_hash is None or len(received_hash) != 32:
                            print("[TCP] Missing hash or connection error while receiving")
                            break
                        computed_hash = hashlib.sha256(chunk).digest()
                        # If the hash is valid, then the block is written
                        if received_hash == computed_hash:
                            file.write(chunk)
                            i += 1
                            server_socket.sendall(b"1")
                        else:
                            self.error_count += 1
                            server_socket.sendall(b"0")
                            print("Hash error detected. Block ignored.")
                    except ConnectionResetError:
                        print("[TCP] Connection reset by Client")
                        break
                        
            # Once reception is complete, the temporary file is renamed
            print(f"[BLOCK] All chunks received\nErrors: {self.error_count} | Chunks OK: {i}")
            final_path = os.path.join(folder_path, final_filename)
            os.rename(temp_file, final_path)
            print(f"[TCP] [{client_ip}] Video saved as {final_filename}\n")
            server_socket.close() 
            self.saved = True
        except Exception as e :
            print(f"[TCP] Error during reception : {e}")
            try :
                # Deletion of temporary file in case of error
                if os.path.exists(temp_file) :
                    os.remove(temp_file)
                    print("[TCP] Temporary file deleted due to an error")
            except :
                pass
            server_socket.close()
        
       
    def start_serv(self):
        '''
        Main loop of the TCP server
        '''
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.server_host, self.server_port))
        server_socket.listen(config.MAX_TCP_CONNECTIONS)
        print("[TCP] Server Started")
        print("--------------------------------------------------------------------")
        print("---------------------------Wait for order---------------------------")
        print("--------------------------------------------------------------------\n")
        self.led.off()
        try :
            while True:
                read, _, _ = select.select(self.connected + [server_socket], [], [])   
                for sock in read:
                    if sock == server_socket:
                        client_socket, addr = server_socket.accept()
                        self.connected.append(client_socket)
                    else:
                        try:
                            self.clientip = sock.getpeername()[0]
                            client_ip = sock.getpeername()[0]
                            save_path = self.video_path+"/"+client_ip
                            threading.Thread(target = self.receive_file, args = (sock, save_path, client_ip), daemon = True).start()
                            self.connected.remove(sock)
                            print(f"\n[TCP] Video stored in {self.video_path +'/'+ client_ip}")
                        except Exception as e:
                            print(f"[TCP] Error during reception : {e}")
                            self.connected.remove(sock)
                            sock.close()
                            
        except Exception as e:
                            print(f"[TCP] Erreur during reception : {e}")
                            self.connected.remove(sock)
                            sock.close()


# ---------------------------- UDP SERVER CLASS --------------------------

class UDPServer:
    def __init__(self, server_port = config.UDP_PORT):
        '''
        UDP server initialization to send commands to clients
        '''
        self.server_port = server_port
        self.client = [(ip, self.server_port) for ip in config.SLAVE_IPS]
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind((config.MASTER_IP, self.server_port))
        self.video_start_times = {}
        self.video_start = None
        self.led = Led(pin = config.LED_PIN)


    def send_message_to_client(self, client, message: str):
        '''
        Send a UDP message
        '''
        self.udp_socket.sendto(message.encode('utf-8'), client)


    def wait_for_fb(self, client, timeout = config.UDP_TIMEOUT):
        '''
        Wait for a response message from a client
        '''
        self.udp_socket.settimeout(timeout)
        try:
            fb, client_address = self.udp_socket.recvfrom(100)
            if fb :
                message = fb.decode().strip()
                if message.startswith("START_TIME:"):
                    self.video_start = message.split("START_TIME:")[1]
                    print (f"[START_TIME] Video from : {client_address[0]} starts at : {self.video_start}")
                    self.video_start_times[client_address[0]]=self.video_start
                return message, client_address
                
            else :
                return None

        except socket.timeout:
            return None  


    def send_order(self, message: str):   
        '''
        Send a command to all clients
        '''
        failure_count = 0
        client_n = self.client[:]
        while len(client_n) > 0 and failure_count < 3 :
            client = client_n.pop(0)
            try:
                self.send_message_to_client(client, message)
                if message == "START_TIME":
                    self.video_start = datetime.strptime(self.wait_for_fb(client)[0], "%Y-%m-%d-%H-%M-%S-%f")
                else :
                    fb, gentleman = self.wait_for_fb(client)
                    if not fb:
                        failure_count += 1
                        client_n.append(client)
                    else :
                        if gentleman != client:
                            client_n.remove(gentleman)
            except Exception:
                failure_count += 1
                client_n.append(client)
            time.sleep(0.1) 


# ------------------------ SLAVE MANAGEMENT CLASS ------------------------

class SlaveManagement:
    def __init__(self, fps = config.FPS, paths = ""):
        '''
        Master/clients management initialization
        '''
        self.slave_ips = config.SLAVE_IPS 
        self.path = paths
        self.session_folder = self.create_session_folder() # Create session folders
        self.pointage_list = [] # List of frame timestamps
        self.running = False # Recording in progress flag
        self.recording = False
        self.udp_server = UDPServer() # UDP server for commands
        self.tcp_server = TCPServer(video_path = self.session_folder) # TCP server to receive video
        self.tcp_server.slave_manager = self
        self.fps=fps
        self._tag_lock = threading.Lock()
        self._last_tag_time = 0.0
        self._min_tag_interval = 0.5
        self.video_start = None
        self.uno_txt = None
        self.tag_pin = config.TAG_PIN
        self.switch_pin = config.SWITCH_PIN
        self.led = Led(pin = config.LED_PIN)
        
        # GPIO setup for tag and switch
        GPIO.setmode(GPIO.BCM)        
        GPIO.setup(self.tag_pin,GPIO.IN, pull_up_down = GPIO.PUD_UP)
        GPIO.add_event_detect(self.tag_pin, GPIO.FALLING, callback = self.mark_frame, bouncetime = 50)        
        GPIO.setup(self.switch_pin,GPIO.IN, pull_up_down = GPIO.PUD_UP)
        GPIO.add_event_detect(self.switch_pin, GPIO.BOTH, bouncetime = 800)
        self.detected_tags = []        
        self.tcp_server.led=self.led
        

    def create_session_folder(self):
        '''
        Create a unique folder for the current session
        '''
        session_id = 0
        path_storage = self.path
        checked = True
        i = 0
        while checked:
            print(f"Session folder created : {checked}")
            try :
                # If already tried multiple times, increment the path
                if i > 0:
                    path_storage = self.path + f"{i}"
                    print(f"Video stored in : {path_storage}")
                # Find the next free session number
                while os.path.exists(os.path.join(path_storage, f"Session_{session_id}")):
                    session_id += 1
                session_path = os.path.join(path_storage, f"Session_{session_id}")
                checked = False
            except : 
                i += 1
        
        # Create the session folder
        os.makedirs(session_path)
        print(f"Video stored in : {session_path}")
        # Create a subfolder for each client
        for ip in self.slave_ips:
            os.makedirs(os.path.join(session_path, ip))
        return session_path


    def record_pointage(self):
        '''
        Record frame timestamps and indices during recording
        '''
        frame_count = 0
        self.global_frame_count = 0
        print("[THREAD] Thread record_pointage has been launched  at :", datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f"))
        print("[Tag] Ready")
        self.led.stop_blink() # Stop LED blinking
        start_time = time.perf_counter() # Initial time to calculate frame intervals
        while self.running:
            current_time = time.perf_counter()
            elapsed = current_time - start_time
            ts_datetime = datetime.now()
            with self._tag_lock : 
                # Save timestamp and frame index
                frame_index = self.global_frame_count
                self.global_frame_count += 1
                self.pointage_list.append((ts_datetime.strftime("%Y-%m-%d-%H-%M-%S-%f"), frame_index, 0))
            frame_count += 1
            # Wait until the next frame according to FPS
            next_frame_time = frame_count / self.fps
            sleep_time = next_frame_time - elapsed
            if sleep_time > 0 : 
                time.sleep(sleep_time)
        print("Thread record_pointage has been stopped at :", datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f"))  
        # Convert the first timestamp to datetime for info.txt
        self.uno_txt = datetime.strptime(self.pointage_list[0][0], "%Y-%m-%d-%H-%M-%S-%f")
        print(f"[INFO.TXT] First time in info.txt : {self.uno_txt}") 
        self.save_pointage()    # Save recorded points 
        print("\n----------------- FRAMES CHECKING -----------------")
        print(f"[CHECK] Registered frames : {frame_count}")
        print("-------------------------------------------------------------\n")
        print("--------------------------------------------------------------------")
        print("-----------------------Wait for new recording-----------------------")
        print("--------------------------------------------------------------------\n")
        self.led.stop_blink()
        self.led.off()


    def mark_frame(self, event = None):
        '''
        Callback to detect a tag (button press)
        '''
        now = datetime.now()
        with self._tag_lock:
            # Check minimum interval between two tags
            if now.timestamp() - self._last_tag_time < self._min_tag_interval:
                return
            
            self._last_tag_time = now.timestamp()
            time.sleep(0.01)
            if GPIO.input(self.tag_pin) == GPIO.LOW :
                time.sleep(0.02)
                if GPIO.input(self.tag_pin) == GPIO.LOW :
                    # Save detected tag
                    self.detected_tags.append(now)
                    print("\n[TAG] The tag has been detected at :", now.strftime("%Y-%m-%d-%H:%M:%S:%f"),"\n")
                        
        
    def save_pointage(self):
        '''
        Save points and apply tags on frames
        '''
        global record_index
        count = 0
        # Wait until TCPServer has finished saving the video
        while (self.tcp_server.saved == False) and count < 100:
            count += 1
            time.sleep(1)
        info_path = os.path.join(self.session_folder, "info.txt")
        if not os.path.exists(info_path):
            with open(info_path, "w") as f:
               pass

        # Initial writing of frame points
        with open(info_path, "a") as f:
            for e in self.pointage_list:
                f.write(f"{e[0]};{e[1]};0\n")
            f.flush()
            os.fsync(f.fileno())
        # Read to apply tags
        with open(info_path, "r") as f:
            lines = f.readlines()
        start_index = len(lines) - len(self.pointage_list)
        new_lines = lines[start_index:]
        detected_tags_sorted = sorted(self.detected_tags)
        tag_index = 0		        
        for i, line in enumerate(new_lines):
            ts_str, frame_index, old_tag = line.strip().split(";")
            ts = datetime.strptime(ts_str, "%Y-%m-%d-%H-%M-%S-%f")
            tag = "0"
            while tag_index < len(detected_tags_sorted) and (ts - detected_tags_sorted[tag_index]).total_seconds() > 1/self.fps:
                tag_index += 1
            if tag_index < len(detected_tags_sorted) and abs((ts - detected_tags_sorted[tag_index]).total_seconds()) <= 1/self.fps:
                tag = "1"
                tag_index += 1
            if tag != old_tag:
                print(f"\n[FINAL TAG]{line.split()}-->{ts_str}--> Frame index : {frame_index}")
            new_lines[i] = f"{ts_str}; {frame_index}; {tag}\n"
        lines[start_index:] = new_lines
        # Rewrite the info.txt file
        with open (info_path, "w") as f:
            f.writelines(lines)
            f.flush()
            os.fsync(f.fileno())
        # Cleanup
        self.detected_tags.clear()
        self.pointage_list.clear()  
        print("") 
        countv1 = 0
        # Wait for video start time
        while self.video_start is None and countv1 <= 20:
            countv1 += 1
            print("Waiting for video_start")
            time.sleep(1)
        # Process all videos from each client
        for ip in self.slave_ips:
            folder = os.path.join(self.session_folder, ip)
            if not os.path.exists(folder):
                continue
                
            countv2 = 0
            Var = False
            while Var == False and countv2 <= 5 :
                Var = True
                if os.listdir(folder)==[] :					
                    print(f"[{folder}] Folder is empty")
                    Var = False
                print("Waiting for files ...")
                if count == 5 :
                    print("Wait too long, passed")
                time.sleep(1)
                countv2 += 1                
            videos = sorted([
                f for f in os.listdir(folder)
                if f.lower().endswith(".mp4") and "_temp" not in f and not f.lower().endswith(".png")
            ])
            if not videos :
                print(f"[{ip}] No video files founded.")
                continue
                
            start_str = self.udp_server.video_start_times.get(ip)
            if not start_str :
                print(f"[{ip}] No start_time found, skipping trimming")
                start_str = "2025-10-09-14-23-05-69"
            try : 
                video_start = datetime.strptime(start_str, "%Y-%m-%d-%H-%M-%S-%f")
            except Exception as e: 
                print(f"[{ip}] Error parsing start time :{e}")
                continue		
            
            # Trim or adjust video for synchronization
            for index, video in enumerate(videos, start = 1):
                if f"video{record_index}" not in video : 
                    continue
                    
                else :
                    path_in = os.path.join(folder, f"video{record_index}_1.mp4")
                    path_trimmed = path_in.replace(".mp4", "_trimmed.mp4")
                    try :
                        print(video)
                        if re.match(rf"video{record_index}_1\.mp4$", video) :
                            if video_start == datetime.strptime("2025-10-09-14-23-05-69", "%Y-%m-%d-%H-%M-%S-%f"):
                                duration_to_trim = 0.0
                            else : 
                                duration_to_trim = (self.uno_txt - video_start).total_seconds() - 0.5
                            print(f"\n[{ip}] Duration to trim : {duration_to_trim:.2f} sec")
                            trim_video(path_in, path_trimmed, duration_to_trim)
                            os.replace(path_trimmed, path_in)
                        else:
                            print(f"[{ip}] No cut : {video} ")
                    except Exception as e :
                        print(f"[{ip}] Error during cutting : {e}")
                        if not os.path.exists(path_in) or os.path.getsize(path_in) < 1024 * 100 :
                            print(f"[{ip}] Corrupted or empty file : {video}, deleting...")
                            os.remove(path_in)
                        continue
                     
                    # Remove temporary files
                    for file in os.listdir(folder):
                        if "_temp" in file:
                            temp_path = os.path.join(folder, file)
                            if os.path.isfile(temp_path):
                                os.remove(temp_path)
                                print(f"[{ip}] Temporary files removed : {file}")
        self.udp_server.video_start_times = {}
        self.tcp_server.saved = False
        record_index += 1
        self.tcp_server.saved_by_ip = {}
        print("[CORRECTION] All videos have been trimmed or corrected\n")
	

    def order(self):
        '''
        Main loop monitoring the switch and sending START / STOP to clients
        '''
        print("[MASTER] Ready to send order")
        while True:
            if GPIO.event_detected(self.switch_pin):
                print("[SWITCH] Switch has been detected at :", datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f"))
                print("[MASTER] Ready to send order")
                if GPIO.input(self.switch_pin) == GPIO.LOW: #ON position
                    if not self.running:
                        self.led.blink(frequency = 3)
                        self.udp_server.send_order("START")
                        self.running = True
                        threading.Thread(target = self.record_pointage, daemon = True).start()
                        print(f"Running = {self.running}")
                        time.sleep(1)
                        self.led.on()                        
                elif GPIO.input(self.switch_pin) == GPIO.HIGH: #OFF position
                    if self.running:
                        self.led.off()
                        self.led.blink(frequency = 3)
                        self.udp_server.send_order("STOP")
                        self.running = False
                        print(f"Running = {self.running}")
                        self.udp_server.send_order("START_TIME")
                        self.video_start = self.udp_server.video_start        
            time.sleep(1)
    

    def run(self):
        '''
        Launch the TCP server and the order loop
        '''
        threading.Thread(target = self.tcp_server.start_serv, daemon = False).start()
        self.order()

# ---------------------------- RECV_EXACT FUNCTION ------------------------

def recv_exact(sock, n, timeout = 10.0):
    '''
    Reads exactly n bytes from a TCP socket with timeout
    Returns None if the client disconnects or exceeds the timeout
    '''
    sock.settimeout(timeout) # Sets a timeout for reading
    data = b''
    try:
        while len(data) < n:
            # Reading the remaining bytes
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None # Client disconnected
                    
            data += chunk # Added the received chunk
    except (socket.timeout, ConnectionResetError):
        return None  # Connection timeout or reset
		
    finally:
        sock.settimeout(None) # Resets the socket to blocking mode
    return data # Returns the complete block received

# ---------------------------- TRIM_VIDEO FUNCTION ------------------------

def trim_video(input_path, output_path, duration_to_trim):
    '''
    Trim or add white frames at the beginning to synchronize the video
    '''
    if not os.path.exists(input_path):
        print(f"Error : Unable to find file : {input_path}.")
        return
    if duration_to_trim >= 0 :
        # Simple trimming with ffmpeg
        cmd = [
            "ffmpeg",
            "-y",
            "-ss", str(duration_to_trim),
            "-i", input_path,
            "-c:v", "libx264", "-preset", "ultrafast",
            "-c:a", "copy",
            output_path
        ]
        subprocess.run(cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        print("[TRIMM] Done")
    else :
        # Add white frames if needed
        cap = cv2.VideoCapture(input_path)
        if not cap.isOpened():
            print(f"[TRIMM][ERROR] Cannot open video: {input_path}")
            return

        fps = cap.get(cv2.CAP_PROP_FPS)
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
        n_white_frames = int(round(-duration_to_trim * fps))
        print(f"[CORRECTION] Adding {n_white_frames} white frames at the beginning.")
        white_frame = 255 * np.ones((height, width, 3), dtype = np.uint8)
        for _ in range(n_white_frames):
            out.write(white_frame)
        while True:
            ret, frame = cap.read()
            if not ret:
                break
                
            out.write(frame)
        cap.release()
        out.release()
        print("[CORRECTION] Video with white frames created:", output_path)
        
            
"----------------------------MAIN METHOD----------------------------------"

if __name__ == "__main__":
    # LED and paths initialization
    led = Led(pin = config.LED_PIN)
    led.off()
    led.blink(frequency = 8)
    path_root = config.PATH_ROOT
    path_storage = config.PATH_STORAGE
    if os.path.exists(path_root) :
        path_root = config.PATH_MEDIA
        all_dir = os.listdir(path_root)
        print("Videos stored on the hard drive")
    else : 
        print("Videos stored on the Raspberry")
        all_dir = []    
    for directory in all_dir :
        print(directory)
        if directory.startswith("Elements") and os.path.isdir(os.path.join(path_root, directory)):
            path_storage = os.path.join(path_root, directory)
            print(path_storage)
            break      
    
    # Create manager and start
    manager = SlaveManagement(fps = config.FPS, paths = path_storage)
    manager.led = led 
    manager.tcp_server.led = led
    manager.run()
