from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.common.by import By
# chrome_driver_path = "C:\Users\shivanandk\Documents\selenium"

d = webdriver.Chrome()
# WebDriver d=new ChromeDriver();
# d.manage().window().fullscreen();
d.get("https://www.instagram.com/")#https://www.instagram.com/
d.maximize_window()
# d.find_element
# d.find_element(xpat)
print("tut")
time.sleep(4)
userName = d.find_element(By.NAME, 'username')
userName.click()
userName.send_keys("shivukaraguppi")
password =d.find_element(By.NAME, 'password')
password.click()
password.send_keys("special@123")
# time.sleep(4)
Login = d.find_element(By.XPATH,"//button[@type='submit']")
Login.click()
time.sleep(5)
print("submit")
Notnow = d.find_element(By.XPATH,"//div[@role='button']")
Notnow.click()
time.sleep(3)
Notnow2 =d.find_element(By.XPATH,"//button[text()='Not Now']")
Notnow2.click()
time.sleep(3)
# Thread.sleep(1000);
findButton = d.find_element(By.XPATH,"//div[@class='x13v4lgv xmn1u35 x10l6tqk xn0lweg x1vjfegm']")#//div[@class='x13v4lgv xmn1u35 x10l6tqk xn0lweg x1vjfegm']
findButton.click()
time.sleep(6)
findButton1 = d.find_element(By.XPATH,"//div[@class='x13v4lgv xmn1u35 x10l6tqk xn0lweg x1vjfegm']")#//div[@class='x13v4lgv xmn1u35 x10l6tqk xn0lweg x1vjfegm']
findButton1.click()
findButton2 = d.find_element(By.XPATH,"//div[contains(text(),'Send message')]")
findButton2.click()
time.sleep(5)
intpu_id_button=d.find_element(By.XPATH,"//input[@placeholder='Search...']")
intpu_id_button.click()
time.sleep(3)
findButton2.send_keys("@nu_radha Karaguppi")#name
# findButton2.click()
time.sleep(3)
chat_botton=d.find_element(By.XPATH,"//span[@class='x1lliihq x1plvlek xryxfnj x1n2onr6 x193iq5w xeuugli x1fj9vlw x13faqbe x1vvkbs x1s928wv xhkezso x1gmr53x x1cpjm7i x1fgarty x1943h6x x1i0vuye xvs91rp xo1l8bm x5n08af x10wh9bi x1wdrske x8viiok x18hxmgj'][normalize-space()='@nu_radha Karaguppi']")
chat_botton.click()
time.sleep(3)
print("enters")
chat_botton1=d.find_element(By.XPATH,"//div[contains(text(),'Chat')]")
chat_botton1.click()
print("enters2")
time.sleep(3)
enterNameclick =d.find_element(By.XPATH,"//p[@class='xat24cr xdj266r']")
enterNameclick.click()
enterNameclick.send_keys("@nu_radha Karaguppi")#msg_to_be_entered
enterNameclick.send_keys(Keys.ENTER)
time.sleep(3)
messageButton= d.find_element(By.XPATH,"//*[contains(text(),'Message') and @role='button']")
time.sleep(3)
# }
d.quit()