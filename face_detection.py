from multiprocessing.connection import wait
import cv2
import pyautogui
import subprocess
import time

time.sleep(9)

def face_dect(retry):
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
    cap = cv2.VideoCapture(0)
    x=0
    while True:
        print(x)
        ret, frame = cap.read()
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = face_cascade.detectMultiScale(gray, scaleFactor=1.3, minNeighbors=5, minSize=(30, 30))
        # cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        if len(faces) > 0:
            pass
        else :
            # print("pekaboo!!!")
            return x+1
            # break   
        for (x, y, w, h) in faces:#display
            cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 7)
            # print(frame)
        if retry==4:
            cmd='rundll32.exe user32.dll, LockWorkStation'
            subprocess.call(cmd)

        # Display the frame with detected faces
        cv2.imshow('Face Detection', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'): #exit loop
            break
    if x==5:
        print("bye...")
        
        cap.release()
        cv2.destroyAllWindows()
        # break 

def kanhu():

    for x in range(5):
        print("retry..."+str(x))
        if x ==5:
            
            break
        retry=face_dect(x)

kanhu()
    
