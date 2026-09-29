# ======================= IMPORTS ===========================
# Gestion du temps (sleep, perf_counter, etc)
import time
# Gestion des fichiers et dossiers
import os
# Gestion du multithreading
import threading
#Communication réseau TCP/UDP
import socket
# Pour écouter plusieurs sockets simultanément
import select
# Gestion des dates et heures
from datetime import datetime
#Hashing pour vérifier l'intégrité des fichiers
import hashlib  
# Gestion des GPIO du Raspberry Pi
import RPi.GPIO as GPIO
import sys
# Force le flush automatique des prints
sys.stdout.reconfigure(line_buffering = True)
# OpenCV pour traitement vidéo
import cv2
# Exécution de commandes externes (ex : ffmpeg)
import subprocess
# Expressions régulières
import re
# Manipulation de tableaux / images
import numpy as np
# Configuration personnalisée du projet
import config_master as config

# Index global pour nommer les vidéos
record_index = 1


# ---------------------------- CLASSE LED ---------------------------------

class Led:
    def __init__(self, pin = config.LED_PIN):
        '''
        Initialisation LED sur le GPIO choisi
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
        Allume la LED
        '''
        self._blinking = False
        self.GPIO.output(self.pin, self.GPIO.HIGH)


    def off(self):
        '''
        Eteint la LED
        '''
        self._blinking = False
        self.GPIO.output(self.pin, self.GPIO.LOW)


    def toggle(self):
        '''
        Inverse l'état actuel de la LED
        '''
        current = self.GPIO.input(self.pin)
        self.GPIO.output(self.pin, not current)


    def blink(self, frequency = config.BLINK_FREQUENCY):
        '''
        Fait clignoter la LED à une fréquence donnée
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
        Arrête le clignotement et éteint la LED
        '''
        self._blinking = False
        if self._blink_thread and self._blink_thread.is_alive():
            self._blink_thread.join(timeout = 0.1)
        self._blink_thread = None
        self.GPIO.output(self.pin, self.GPIO.LOW)


    def cleanup(self):
        '''
        Nettoie le GPIO
        '''
        self.stop_blink()
        self.GPIO.cleanup(self.pin)


# ---------------------------- CLASSE TCP SERVER --------------------------

class TCPServer:
    def __init__(self, server_host = config.MASTER_IP, server_port = config.TCP_PORT, video_path = os.path.join(config.PATH_ROOT, config.VIDEO_FOLDER_NAME)):
        '''
        Initialisation serveur TCP pour recevoir les vidéos
        '''
        self.server_host = server_host
        self.server_port = server_port
        self.video_path = video_path
        self.connected = [] # Liste des clients connectés
        self.error_count = 0 # Compteur d'erreurs de réception
        self.saved = False # Flag si la vidéo est sauvgardée
        self.clientip = None

		
    def receive_file(self, server_socket, folder_path, client_ip):
        '''
        Reçoit une vidéo en plusieurs blocs avec vérification SHA256
        '''
        global record_index
        self.saved = False
        i = 0
        os.makedirs(folder_path, exist_ok = True)
        # Recherche du dernier index de fichier pour incrémenter correctement le nom
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
                        # Lecture de la taille du bloc (4 octets)
                        raw_len = recv_exact(server_socket, 4)
                        if raw_len is None:
                            break

                        block_len = int.from_bytes(raw_len, 'big')
                        if block_len <= 0 : 
                            server_socket.sendall(b"1")
                            break
                        
                        # Lecture du bloc de données complet
                        chunk = b''
                        while len(chunk) < block_len:
                            packet = recv_exact(server_socket, (block_len - len(chunk)))
                            if not packet:
                                raise ConnectionResetError("[TCP] Client disconnected mid-chunk")
                               
                            chunk += packet
                        if len(chunk) != block_len:
                            print("[TCP] Incomplete block, stopping...")
                            break
 
                        # Lecture et vérification du hash SHA256
                        received_hash = recv_exact(server_socket, 32)
                        if received_hash is None or len(received_hash) != 32:
                            print("[TCP] Missing hash or connection error while receiving")
                            break
                        computed_hash = hashlib.sha256(chunk).digest()
                        # Si le hash est valide, alors on écrit le bloc
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
                        
            # Une fois la réception terminée, on renomme le fichier temporaire
            print(f"[BLOCK] All chunks received\nErrors: {self.error_count} | Chunks OK: {i}")
            final_path = os.path.join(folder_path, final_filename)
            os.rename(temp_file, final_path)
            print(f"[TCP] [{client_ip}] Video saved as {final_filename}\n")
            server_socket.close() 
            self.saved = True
        except Exception as e :
            print(f"[TCP] Error during reception : {e}")
            try :
                # Suppression du fichier temporaire en cas d’erreur
                if os.path.exists(temp_file) :
                    os.remove(temp_file)
                    print("[TCP] Temporary file deleted due to an error")
            except :
                pass
            server_socket.close()
        
       
    def start_serv(self):
        '''
        Boucle principale du serveur TCP
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


# ---------------------------- CLASSE UDP SERVER --------------------------

class UDPServer:
    def __init__(self, server_port = config.UDP_PORT):
        '''
        Initialisation serveur UDP pour envoyer des commandes aux clients
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
        Envoi d'un message UDP
        '''
        self.udp_socket.sendto(message.encode('utf-8'), client)


    def wait_for_fb(self, client, timeout = config.UDP_TIMEOUT):
        '''
        Attente d'un retour de message d'un client
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
        Envoi d'une commande à tous les clients
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


# ------------------------ CLASSE SLAVE MANAGEMENT ------------------------

class SlaveManagement:
    def __init__(self, fps = config.FPS, paths = ""):
        '''
        Initialisation de la gestion master/clients
        '''
        self.slave_ips = config.SLAVE_IPS 
        self.path = paths
        self.session_folder = self.create_session_folder() # Création de dossiers session
        self.pointage_list = [] # Liste des timestamps de frames
        self.running = False # Flag enregistrement en cours
        self.recording = False
        self.udp_server = UDPServer() # Serveur UDP pour les commandes
        self.tcp_server = TCPServer(video_path = self.session_folder) # Serveur TCP pour la réception vidéo
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
        
        # Setup GPIO pour tag et switch
        GPIO.setmode(GPIO.BCM)        
        GPIO.setup(self.tag_pin,GPIO.IN, pull_up_down = GPIO.PUD_UP)
        GPIO.add_event_detect(self.tag_pin, GPIO.FALLING, callback = self.mark_frame, bouncetime = 50)        
        GPIO.setup(self.switch_pin,GPIO.IN, pull_up_down = GPIO.PUD_UP)
        GPIO.add_event_detect(self.switch_pin, GPIO.BOTH, bouncetime = 800)
        self.detected_tags = []        
        self.tcp_server.led=self.led
        

    def create_session_folder(self):
        '''
        Crée un dossier unique pour la session actuelle
        '''
        session_id = 0
        path_storage = self.path
        checked = True
        i = 0
        while checked:
            print(f"Session folder created : {checked}")
            try :
                # Si on a déjà essayé plusieurs fois, on incrémente le chemin
                if i > 0:
                    path_storage = self.path + f"{i}"
                    print(f"Video stored in : {path_storage}")
                # Trouve le prochain numéro de session libre
                while os.path.exists(os.path.join(path_storage, f"Session_{session_id}")):
                    session_id += 1
                session_path = os.path.join(path_storage, f"Session_{session_id}")
                checked = False
            except : 
                i += 1
        
        # Création du dossier session
        os.makedirs(session_path)
        print(f"Video stored in : {session_path}")
        # Création d'un sous-dossier pour chaque client
        for ip in self.slave_ips:
            os.makedirs(os.path.join(session_path, ip))
        return session_path


    def record_pointage(self):
        '''
        Enregistreles timestamps et indices de frames pendant l'enregistrement
        '''
        frame_count = 0
        self.global_frame_count = 0
        print("[THREAD] Thread record_pointage has been launched  at :", datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f"))
        print("[Tag] Ready")
        self.led.stop_blink() # Stoppe le clignotement LED
        start_time = time.perf_counter() # Temps initial pour calculer les intervalles de frame
        while self.running:
            current_time = time.perf_counter()
            elapsed = current_time - start_time
            ts_datetime = datetime.now()
            with self._tag_lock : 
                # Sauvegarde timestamp et index de frame
                frame_index = self.global_frame_count
                self.global_frame_count += 1
                self.pointage_list.append((ts_datetime.strftime("%Y-%m-%d-%H-%M-%S-%f"), frame_index, 0))
            frame_count += 1
            # Attente jusqu'à la prochaine frame selon le FPS
            next_frame_time = frame_count / self.fps
            sleep_time = next_frame_time - elapsed
            if sleep_time > 0 : 
                time.sleep(sleep_time)
        print("Thread record_pointage has been stopped at :", datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f"))  
        # Conversion du premier timestamp en datetime pour info.txt
        self.uno_txt = datetime.strptime(self.pointage_list[0][0], "%Y-%m-%d-%H-%M-%S-%f")
        print(f"[INFO.TXT] First time in info.txt : {self.uno_txt}") 
        self.save_pointage()    # Sauvegarde les points relevés    
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
        Callback pour détecter un tag (pression du bouton)
        '''
        now = datetime.now()
        with self._tag_lock:
            # Vérifie l'intervalle minimum entre deux tags
            if now.timestamp() - self._last_tag_time < self._min_tag_interval:
                return
            
            self._last_tag_time = now.timestamp()
            time.sleep(0.01)
            if GPIO.input(self.tag_pin) == GPIO.LOW :
                time.sleep(0.02)
                if GPIO.input(self.tag_pin) == GPIO.LOW :
                    # Enregistre le tag détecté
                    self.detected_tags.append(now)
                    print("\n[TAG] The tag has been detected at :", now.strftime("%Y-%m-%d-%H:%M:%S:%f"),"\n")
                        
        
    def save_pointage(self):
        '''
        Sauvegarde les points et applique les tags sur les frames
        '''
        global record_index
        count = 0
        # Attend que le TCPServer ait fini de sauvegarder la vidéo
        while (self.tcp_server.saved == False) and count < 100:
            count += 1
            time.sleep(1)
        info_path = os.path.join(self.session_folder, "info.txt")
        if not os.path.exists(info_path):
            with open(info_path, "w") as f:
               pass

        # Ecriture initiale des points de frames
        with open(info_path, "a") as f:
            for e in self.pointage_list:
                f.write(f"{e[0]};{e[1]};0\n")
            f.flush()
            os.fsync(f.fileno())
        # Lecture pour appliquer les tags
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
        # Réécriture du fichier info.txt
        with open (info_path, "w") as f:
            f.writelines(lines)
            f.flush()
            os.fsync(f.fileno())
        # Nettoyage
        self.detected_tags.clear()
        self.pointage_list.clear()  
        print("") 
        countv1 = 0
        # Attente du start_time vidéo
        while self.video_start is None and countv1 <= 20:
            countv1 += 1
            print("Waiting for video_start")
            time.sleep(1)
        # Traitement de toutes les vidéos de chaque client
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
            
            # Découpe ou ajuste la vidéo pour la synchronisation
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
                     
                    # Suppression fichiers temporaires
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
        Boucle principale qui surveille le switch et envoie START / STOP aux clients
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
        Lance le serveur TCP et la boucle d'ordre
        '''
        threading.Thread(target = self.tcp_server.start_serv, daemon = False).start()
        self.order()

# ---------------------------- FONCTION RECV_EXACT ------------------------

def recv_exact(sock, n, timeout = 10.0):
    '''
    Lit exactement n octets depuis un socket TCP avec timeout.
    Retourne None si le client se déconnecte ou dépasse le délai.
    '''
    sock.settimeout(timeout) # Fixe un timeout pour la lecture
    data = b''
    try:
        while len(data) < n:
            # Lecture des octets restants
            chunk = sock.recv(n - len(data))
            if not chunk:
                return None # Client déconnecté
                    
            data += chunk # Ajout du chunk reçu
    except (socket.timeout, ConnectionResetError):
        return None  # Timeout ou reset de connexion
		
    finally:
        sock.settimeout(None) # Rétablit le socket en mode bloquant
    return data # Retourne le bloc complet reçu

# ---------------------------- FONCTION TRIM_VIDEO ------------------------

def trim_video(input_path, output_path, duration_to_trim):
    '''
    Découpe ou ajoute des frames blanches au début pour synchroniser la vidéo
    '''
    if not os.path.exists(input_path):
        print(f"Error : Unable to find file : {input_path}.")
        return
    if duration_to_trim >= 0 :
        # Découpe simple avec ffmpeg
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
        # Ajout de frames blanches si besoin
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
    # Initialisation LED et paths
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
    
    # Création du manager et lancement
    manager = SlaveManagement(fps = config.FPS, paths = path_storage)
    manager.led = led 
    manager.tcp_server.led = led
    manager.run()
