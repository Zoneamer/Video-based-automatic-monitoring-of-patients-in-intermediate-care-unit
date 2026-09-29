# ======================= IMPORTS ===========================
# Gestion du multithreading pour exécuter plusieurs tâches simultanément
import threading
# OpenCV pour le traitement vidéo et la manipulation de frames
import cv2
# Gestion du temps (sleep, perf_counter, etc)
import time
# Gestion des dates et heures
from datetime import datetime 
# Bibliothèque pour contrôler la caméra
from picamera2 import Picamera2
# Communication réseau TCP/UDP
import socket
# Gestion des fichiers et dossiers
import os
# Interaction avec l'interpréteur Python 
import sys
# Hashing pour vérifier l'intégrité des fichiers
import hashlib
# Configuration personnalisée pour ce client
import config_client as config
# Gestion des fichiers échoués
import shutil

# Force le flush automatique des prints
sys.stdout.reconfigure(line_buffering = True)


# ---------------------------- CLASSE UDP CLIENT --------------------------
class UDPClient:
    def __init__(self, server_port = config.UDP_SERVER_PORT, timeout = config.UDP_TIMEOUT):
        '''
        Initialisation du client UDP pour communiquer avec le master
        '''
        self.server_address = (config.UDP_SERVER_IP, server_port)
        self.server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.server.bind((config.UDP_SERVER_IP, server_port))
        self.ip = config.UDP_SERVER_IP
        self.adClient = None


    def send_command(self, command):
        '''
        Envoie une commande au master via UDP
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
        Ecoute une commande reçue du master
        '''
        (data, self.adClient) = self.server.recvfrom(1024)
        return data.decode()


    def close(self):
        '''
        Ferme la connexion UDP
        '''
        self.server.close()
        print("[UDP] Connexion closed.")


# ---------------------------- CLASSE TCP CLIENT --------------------------
class TCPClient:
    '''
    Initialisation du client TCP pour envoyer des vidéos
    '''
    def __init__(self, server_host = config.TCP_MASTER_IP, server_port = config.TCP_MASTER_PORT):
        self.server_host = server_host
        self.server_port = server_port
        self.client_socket = None
        self.ip = config.UDP_SERVER_IP


    def connect(self):
        '''
        Etablit la connexion TCP avec le serveur master
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
        Envoie un fichier vidéo en blocs avec vérification SHA256
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
                        # Fin de fichier, envoi du chunk 0 pour signaler la fin
                        while True:
                            client_socket.sendall((0).to_bytes(4, "big"))
                            self.client_socket.settimeout(0.5)
                            try :
                                ack = self.client_socket.recv(1)
                                if ack == b"1":
                                    break
                            
                            except socket.timeout:
                                pass
                        break
    
                    # Calcul du hash du bloc pour vérification
                    chunk_hash = hashlib.sha256(chunk).digest()
                    length_bytes = len(chunk).to_bytes(4, "big")
                    # Attente de l'accusé de récep
                    retry = 0
                    while retry < 5:
                        client_socket.sendall(length_bytes)
                        client_socket.sendall(chunk)
                        client_socket.sendall(chunk_hash)
                        self.client_socket.settimeout(0.5)
                        try :
                            # Attente de l'accusé de réception
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
            # Déplacer le fichier échoué dans un autre dossier
            failed_folder = os.path.join(os.path.dirname(file_path), "failed_videos")
            os.makedirs(failed_folder, exist_ok = True)
            try : 
                shutil.move(file_path, failed_folder)
                print(f"[TCP] Incomplete file moved to failed_videos : {file_path}")
            except Exception as ee :
                print(f"[WARNING] Could not move failed file : {ee}")


# ---------------------------- CLASSE CAMERA ------------------------------
class Camera:
    def __init__(self, host = config.TCP_MASTER_IP, fps = config.FPS, resolution = config.RESOLUTION,
                 save_dir = config.SAVE_DIR, segment_duration = config.SEGMENT_DURATION):
        '''
        Initialisation de la caméra et des paramètres d'enregistrement
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
        Initialise et démarre la caméra Picamera2
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
        Crée un nouveau segment vidéo pour l'enregistrement
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
        Démarre l'enregistrement vidéo
        '''
        self.start_camera()
        self.create_new_segment()
        self.video_start_info = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        self.video_start = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")
        print(f"[CAMERA] Video starts (saved for trimm) at : {self.video_start_info}")


    def stop_recording(self):
        '''
        Stoppe l'enregistrement vidéo et libère les ressources
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
        Envoie toutes les vidéos enregistrées au serveur TCP
        Après l'envoi, supprime les fichiers locaux
        '''
        print("Sending videos started at :", datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f"))
        self.tcp_client.connect()
        for path in self.video_path_list[:]:
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
        Boucle principale d'enregistrement
        - Gère les segments vidéo
        - Corrige la dérive temporelle
        - Ecrit les frames dans le fichier
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
                        # Fin de segment, création d'un nouveau segment
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
                        # Reset des compteurs et synchronisations
                        frame_index = 0
                        self.start_perf = time.perf_counter()
                        self.last_perf_sync = self.start_perf
                        self.last_datetime_sync = datetime.now()
                        segment_start_perf = self.start_perf 
                        segment_start_real = datetime.now()
                        next_drift_correction = self.start_perf + DRIFT_CORRECTION_INTERVAL
                        print("[RESET] Timing base reinitialized at new segment start\n")
                        continue
                    
                    # Capture d'une frame
                    frame = None
                    wait_start = time.perf_counter()
                    max_wait = config.MAX_FRAME_WAIT
                    while frame is None and (time.perf_counter() - wait_start) < max_wait:
                        frame = self.camera.capture_array()
                    if frame is not None:
                        self.video_writer.write(frame)
                    # Correction de dérive périodique
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
                    # Calcul du temps à attendre pour la prochaine frame - maintient du fps
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
        Traite les commandes reçues du master
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
                # Renvoi de l'heure de début au master
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
        Boucle d'écoute des commandes UDP
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
        Démarre les threads pour l'écoute UDP et l'enregistrement continu
        '''
        threading.Thread(target = self.listen_udp, daemon = False).start()
        threading.Thread(target = self.record_loop, daemon = False).start()



# ---------------------------- BLOC PRINCIPAL ----------------------------
if __name__ == "__main__":
    # Affiche l'identifiant de la caméra et l'heure de lancement
    print(f"Camera {config.NUMERO} :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "\n")
    # Nettoie le dossier de sauvegarde existan
    for file in os.listdir(config.SAVE_DIR):
        if os.path.isfile(os.path.join(config.SAVE_DIR, file)):
            os.remove(os.path.join(config.SAVE_DIR, file))
    # Initialisation de la caméra et lancement des threads
    cam = Camera()
    cam.start_threads()